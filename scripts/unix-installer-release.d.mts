import type { PathLike } from "node:fs";
export function unixInstallerBytes(source: Buffer): Buffer;
export function publishUnixInstaller(file: PathLike): Promise<void>;
