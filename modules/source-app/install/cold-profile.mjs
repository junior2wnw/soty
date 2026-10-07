// Independently measured Root-owned Source image. Immutable private operator
// profile for this cold acceptance, not publisher metadata or user authority.
export const SOURCE_COLD_PROFILE_V2=Object.freeze({
  id:'soty.ordinary-source.physical-cold',version:2,
  imageId:'sha256:407e9eb50dccc6a55f34f6d1c2ce85f3f2828849856208fef42915ccb1df2562',
  sourceCommit:'e8d9fc8d411e02cca0c9409b3ae59a19323e4950',
  packetManifestSha256:'ff7bb3ef725232c9ce22fdf69572af00f18755faa1374cd19ecd4f4753dd4a11',
  supervisorImage:'sha256:8fd1a16e5239acbe8be0377489a9cd0cac7cf56e73c1be8e5f6a4762bc9e7725',
  dockerBinary:'/usr/bin/docker',socket:'/run/soty-docker.sock',socketGid:1001,
});
export const SOURCE_COLD_PROFILE=Object.freeze({...SOURCE_COLD_PROFILE_V2,version:3,
  imageId:'sha256:ffce4d753b8025e5b7d8a150b0c2a46d48a18f0c027685ca31c1d33a82506f23',
  sourceCommit:'b589ee5b2df49ef7660cf03e9de6a39d229d1cf9',
  packetManifestSha256:'bbe22fb377968928c6819f10c90bf4c1fea8a2413b7972026512ad037ecd7fb6',
});
