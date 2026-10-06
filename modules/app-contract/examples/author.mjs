import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { fixtureConfiguration, pin } from './fixture.mjs';
import { createAuthorDraft, materializeAuthorDraft } from '../sdk.mjs';
import { createAdmissionHost, beginAdmission, closeAdmissionHost } from '../index.mjs';
import { cliError } from '../cli.mjs';

/** Operator-owned synthetic configuration, never an author or HTTP request. */
export function authorFixtureConfiguration() {
  const { descriptor, hostConfig } = fixtureConfiguration();
  return { ...hostConfig, authorProfile: { ...pin('fixture:author/private-ui', '5'), feedback: descriptor.feedback } };
}
export function runAuthorExample(destination) {
  const draft = createAuthorDraft({ title: 'Мой первый проект' });
  const config = authorFixtureConfiguration(), host = createAdmissionHost(config);
  try {
    const descriptor = materializeAuthorDraft(host, draft), admission = beginAdmission(host, descriptor, 'fixture.author-request');
    if (destination) {
      const folder = resolve(destination); mkdirSync(folder);
      mkdirSync(join(folder, '.soty'));
      const write = (name, value) => writeFileSync(join(folder, name), JSON.stringify(value, null, 2) + '\n', { encoding: 'utf8', flag: 'wx' });
      write('.soty/author.json', draft);
      write('.soty/agent.json', descriptor);
      write('fixture-host.json', config);
      write('admission-proposal.json', admission.plan);
    }
    return { ok: true, prototype: true, productionAdmission: false, draft,
      capabilities: descriptor.capabilities.length, feedback: 'required', reviews: descriptor.reviews.mode,
      gates: admission.plan.gates, providerCalls: 0, executedHandlers: 0 };
  } finally { closeAdmissionHost(host); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv.length > 3) throw new Error('arguments');
    process.stdout.write(JSON.stringify(runAuthorExample(process.argv[2])) + '\n');
  } catch (error) { process.stderr.write(JSON.stringify(cliError(error)) + '\n'); process.exitCode = 1; }
}
