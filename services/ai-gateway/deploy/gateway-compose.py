#!/usr/bin/python3 -I
"""Pinned Gateway-only Compose entry point. Default is a read-only check."""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import sys

IMAGE = 'sha256:d2b7c2468c95e6112663ad8c772a868ea295101ee4d05df5186890b6f291466d'
REVISION = 'eeb2cb75660af60247df82ba19ae3f9fd83fbfed'
BASE = '/home/ai2/inference-commerce/releases/f3c92cbed00c0d0f6182fc28447c911aedc9247d/infra/compose.yaml'
OVERLAY = '/home/ai2/.config/soty/account-ai-20261025/gateway-compose.override.yaml'
PRIVATE = '/home/ai2/.config/soty/account-ai-20261025/bundle/gateway.json'
ENV_FILE = '/home/ai2/.config/inference-commerce/runtime.env'
PINS = {
    BASE: '9207571bc87a3ab19a61d834b05a16069666e9325a57a622a33f3fe356ff01fb',
    OVERLAY: 'f8bf26024aad9b13d171af93bad41b3a376469df80f65382da8337bf9dddbbc7',
    PRIVATE: '9a2a6617a7be39f7a48a617f1b0908c1b28ee04624a5a7c2776e189596253b44',
}
GATEWAY_CONFIG = '7b39632e07256545b876754e77a61814df006bd33be7f345b25262d4467bf724'
POSTGRES_CONFIG = '6f2a3d4510e80312a8017708dc8ef342385ae1ccd802a95bb5336531785dfd2c'
DOCKER = ['/usr/bin/docker', '--host', 'unix:///var/run/docker.sock']
ENV = {'HOME': '/home/ai2', 'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
       'LANG': 'C.UTF-8', 'RUNTIME_DIR': '/home/ai2/.config/inference-commerce', 'RELEASE_ID': REVISION}
# No caller-supplied -f/image/network arguments. The qualified overlay is last.
COMPOSE = [*DOCKER, 'compose', '--project-name', 'inference-commerce', '--env-file', ENV_FILE,
           '-f', BASE, '-f', OVERLAY]


class GuardError(Exception):
    pass


def require(ok, code):
    if not ok:
        raise GuardError(code)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def digest(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def command(args, *, timeout=45, parse=True):
    result = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True, env=ENV, timeout=timeout)
    require(result.returncode == 0, 'docker_or_compose_failed')
    return json.loads(result.stdout) if parse else None


def validate_rendered(config):
    gateway = config['services']['gateway']
    require(gateway.get('image') == IMAGE and not gateway.get('build') and gateway.get('pull_policy') == 'never', 'gateway_image_pin_mismatch')
    require(set(gateway.get('networks', {})) == {'default', 'soty-account-ai'}, 'gateway_network_pin_mismatch')
    require(config['networks']['soty-account-ai'].get('external') is True
            and config['networks']['soty-account-ai'].get('name') == 'soty-account-ai', 'private_network_pin_mismatch')
    require(digest(gateway) == GATEWAY_CONFIG, 'gateway_configuration_pin_mismatch')
    require(digest(config['services']['postgres']) == POSTGRES_CONFIG, 'postgres_configuration_pin_mismatch')
    return gateway


RUNTIME_CHECK = r'''
import fs from 'node:fs';import{createHash}from'node:crypto';import pg from'/app/node_modules/pg/lib/index.js';
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1});
try{
 await pool.query('BEGIN READ ONLY');
 const running=Number((await pool.query("SELECT count(*) n FROM inference_requests WHERE status='running'")).rows[0].n);
 const holds=Number((await pool.query("SELECT count(*) n FROM service_budget_holds WHERE state='active'")).rows[0].n);
 await pool.query('ROLLBACK');
 const ready=await fetch('http://127.0.0.1:8080/readyz',{signal:AbortSignal.timeout(3000)});
 const configSha256=createHash('sha256').update(fs.readFileSync('/run/secrets/soty-account-ai-gateway.json')).digest('hex');
 console.log(JSON.stringify({ok:ready.ok,runningInference:running,activeHolds:holds,configSha256}));
}catch{console.log(JSON.stringify({ok:false}));process.exitCode=1;}finally{await pool.end();}
'''


def check():
    for path, expected in PINS.items():
        require(sha(pathlib.Path(path).read_bytes()) == expected, 'deployment_file_pin_mismatch')
    image = command([*DOCKER, 'image', 'inspect', IMAGE])[0]
    require(image['Id'] == IMAGE and image['Config'].get('Labels', {}).get('org.opencontainers.image.revision') == REVISION, 'image_revision_pin_mismatch')
    gateway = validate_rendered(command([*COMPOSE, 'config', '--format', 'json']))
    live = command([*DOCKER, 'inspect', 'inference-commerce-gateway-1'])[0]
    require(live['Image'] == IMAGE and live['State']['Running'], 'serving_gateway_pin_mismatch')
    env = dict(value.split('=', 1) for value in live['Config']['Env'])
    configured = gateway['environment']
    require(all(str(value) == env.get(name) for name, value in configured.items()), 'serving_environment_mismatch')
    require(set(env) - set(configured) <= {'PATH', 'NODE_VERSION', 'YARN_VERSION'}, 'unrecorded_environment_override')
    require(set(live['NetworkSettings']['Networks']) == {'inference-commerce_default', 'soty-account-ai'}, 'serving_network_mismatch')
    private = command([*DOCKER, 'network', 'inspect', 'soty-account-ai'])[0]
    require(private['Internal'] and private['Driver'] == 'bridge'
            and private.get('Labels', {}).get('soty.account-ai.owner') == '20261025'
            and [entry.get('Subnet') for entry in private['IPAM']['Config']] == ['10.203.86.0/28'], 'private_network_policy_mismatch')
    expected = {item['target']: (item['source'], not item.get('read_only', False)) for item in gateway['volumes']}
    actual = {item['Destination']: (item['Source'], item['RW']) for item in live['Mounts']}
    require(actual == expected, 'serving_mounts_mismatch')
    runtime = command([*DOCKER, 'exec', live['Id'], 'node', '--input-type=module', '-e', RUNTIME_CHECK])
    require(runtime.get('ok') and runtime.get('configSha256') == PINS[PRIVATE], 'serving_readiness_or_config_mismatch')
    return {'ok': True, 'image': IMAGE, 'revision': REVISION, 'containerId': live['Id'],
            'overlayLast': True, 'gatewayConfigurationSha256': GATEWAY_CONFIG, 'postgresConfigurationSha256': POSTGRES_CONFIG,
            'configurationValuesExposed': False, 'runningInference': runtime['runningInference'], 'activeHolds': runtime['activeHolds']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true', help='validate pins and serving configuration; default')
    mode.add_argument('--up', action='store_true', help='explicit Gateway-only replacement after the same guards')
    args = parser.parse_args()
    result = check()
    if args.up:
        require(result['runningInference'] == 0 and result['activeHolds'] == 0, 'gateway_not_quiescent')
        # Never starts/recreates dependencies, rebuilds, pulls another image,
        # passes arbitrary options, or starts the retained old Gateway.
        command([*COMPOSE, 'up', '-d', '--no-deps', '--no-build', '--pull', 'never', 'gateway'], timeout=180, parse=False)
        result = check()
    print(json.dumps({**result, 'mode': 'up' if args.up else 'check', 'composeUpExecuted': args.up}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'ok': False, 'code': str(error) if isinstance(error, GuardError) else 'gateway_bootstrap_failed', 'configurationValuesExposed': False}))
        sys.exit(1)
