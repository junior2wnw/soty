import test from 'node:test';
import assert from 'node:assert/strict';
import {assertInstalledSourceImage,SOURCE_PARENT_IMAGE,SOURCE_READER_LABEL} from '../install/image-guard.mjs';
import {storageReaders,currentStorageReaders} from '../../../deploy/connector/storage-guard.mjs';

test('Source image has own exact revision/Reader3/packet witness and cannot satisfy Root rollback readers',()=>{
  const expected={imageId:'sha256:'+'a'.repeat(64),sourceCommit:'b'.repeat(40),packetManifestSha256:'c'.repeat(64)};
  const labels={'org.opencontainers.image.revision':expected.sourceCommit,'io.soty.source.revision':expected.sourceCommit,'io.soty.source.packet.sha256':expected.packetManifestSha256,
    'io.soty.source.parent.oci':SOURCE_PARENT_IMAGE,'io.soty.source.native.readers':SOURCE_READER_LABEL,'io.soty.source.installer':'soty.ordinary-source.operator.v1',
    'io.soty.storage.readers':'','io.soty.universal.legacy':'source-only','io.soty.connect.tree':''};
  const image={Id:expected.imageId,Config:{User:'1000:1000',WorkingDir:'/app/source-app',Entrypoint:['/usr/local/bin/node','/app/source-app/install.mjs'],Labels:labels}};
  assert.equal(assertInstalledSourceImage(image,expected).rootImage,false);assert.throws(()=>storageReaders(image),error=>error.code==='storage_reader_unknown');
  for(const key of Object.keys(labels))assert.throws(()=>assertInstalledSourceImage({...image,Config:{...image.Config,Labels:{...labels,[key]:'wrong'}}},expected));
  const root={...image,Config:{...image.Config,Labels:{'org.opencontainers.image.revision':'d'.repeat(40),'io.soty.storage.readers':currentStorageReaders,'io.soty.universal.legacy':'0'}}};
  assert.throws(()=>assertInstalledSourceImage(root,expected));assert.doesNotThrow(()=>storageReaders(root));
});
