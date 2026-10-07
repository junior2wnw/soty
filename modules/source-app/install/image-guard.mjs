import {check,fields} from '../server/wire.mjs';
export const SOURCE_PARENT_IMAGE='sha256:8fd1a16e5239acbe8be0377489a9cd0cac7cf56e73c1be8e5f6a4762bc9e7725';
export const SOURCE_READER_LABEL='{"schema":"soty.source-native-readers.v1","readers":[1,2,3],"current":3,"legacyRefuses":2}';
/** Constructor-only measured image witness, never publisher manifest rights.
 * Root admission and actual Source literal ReaderCLI are separate checks. */
export function assertInstalledSourceImage(actual,input){
  const expected=fields(input,['imageId','sourceCommit','packetManifestSha256']);
  check(/^sha256:[a-f0-9]{64}$/u.test(expected.imageId)&&/^[a-f0-9]{40}$/u.test(expected.sourceCommit)
    &&/^[a-f0-9]{64}$/u.test(expected.packetManifestSha256),'source_install_image_denied',503);
  const cfg=actual?.Config,labels=cfg?.Labels;
  check(actual?.Id===expected.imageId&&cfg?.User==='1000:1000'&&cfg.WorkingDir==='/app/source-app'
    &&JSON.stringify(cfg.Entrypoint)===JSON.stringify(['/usr/local/bin/node','/app/source-app/install.mjs'])
    &&labels?.['org.opencontainers.image.revision']===expected.sourceCommit&&labels?.['io.soty.source.revision']===expected.sourceCommit
    &&labels?.['io.soty.source.packet.sha256']===expected.packetManifestSha256&&labels?.['io.soty.source.parent.oci']===SOURCE_PARENT_IMAGE
    &&labels?.['io.soty.source.native.readers']===SOURCE_READER_LABEL&&labels?.['io.soty.source.installer']==='soty.ordinary-source.operator.v1'
    &&labels?.['io.soty.storage.readers']===''&&labels?.['io.soty.universal.legacy']==='source-only'&&labels?.['io.soty.connect.tree']==='',
    'source_install_image_denied',503);
  return Object.freeze({schema:'soty.source-image-witness.v1',imageId:expected.imageId,sourceCommit:expected.sourceCommit,
    packetManifestSha256:expected.packetManifestSha256,nativeReader:3,rootImage:false,productionReady:false});
}
