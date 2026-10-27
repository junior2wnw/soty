// One finite public SDK layout, shared by the portable builder, publisher and
// release verifier. No directory glob can accidentally include operator data.
export const SOURCE_APP_COPY_FILES=Object.freeze(Object.fromEntries([
  ...['app.js','app.css','processing.js','feedback-inbox.js'].map(name=>['assets/'+name,'examples/ordinary-app/assets/'+name]),
  ...['domain.mjs','assets/embed.html','assets/ui.js','assets/ui.css'].map(name=>['templates/reading-list/'+name,'examples/reading-list/'+name]),
  ...['soty_native_entry.py','requirements.txt'].map(name=>['python/'+name,'python/'+name]),
]));
export const SOURCE_APP_OUTPUTS=Object.freeze(['server.mjs','browser.mjs','example.mjs','install.mjs',...Object.keys(SOURCE_APP_COPY_FILES)].sort());
export const SOURCE_SDK_TEXT_FILES=Object.freeze([
  'README.md','install/README.md','install/operator.template.json','install/operator.workspace.template.json',
  'types/server.d.mts','types/browser.d.mts','types/example.d.mts','types/processing.d.mts','types/source-rp.d.mts',
].sort());
export const SOURCE_SDK_FILES=Object.freeze([
  'package.json','package-lock.json','npm-shrinkwrap.json','SDK-CAPSULE.json','dist/provenance.json',
  ...SOURCE_APP_OUTPUTS.map(name=>'dist/'+name),...SOURCE_SDK_TEXT_FILES,
].sort());
export const SOURCE_SDK_INDEX='sdk/source-app/index.json';
export const SOURCE_SDK_LIMITS=Object.freeze({archiveBytes:4194304,plainBytes:16777216,fileBytes:4194304,files:128,archives:256,retainedBytes:67108864});
export const sourceSdkArchiveName=name=>typeof name==='string'&&/^sdk\/source-app\/[a-f0-9]{64}\.tgz$/u.test(name);
export const sourceSdkVersion=value=>typeof value==='string'&&/^0\.1\.0-preview\.[1-9][0-9]*$/u.test(value);
