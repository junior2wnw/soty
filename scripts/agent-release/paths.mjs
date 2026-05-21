import { join } from "node:path";

export function createAgentReleasePaths(root) {
  const outputDir = join(root, "public", "agent");
  const windowsReinstallDir = join(outputDir, "windows-reinstall");
  return {
    sourcePath: join(root, "scripts", "soty-agent.mjs"),
    outputDir,
    outputPath: join(outputDir, "soty-agent.mjs"),
    manifestPath: join(outputDir, "manifest.json"),
    windowsMachineCmdPath: join(outputDir, "install-windows-machine.cmd"),
    windowsReinstallDir,
    retiredOpsSkillArtifacts: [
      join(outputDir, "ops-skill.zip"),
      join(outputDir, "ops-skill.tar.gz"),
      join(outputDir, "native-window-chrome")
    ],
    windowsReinstallScriptSpecs: [
      {
        name: "managed",
        fileName: "soty-managed-windows-reinstall.ps1",
        sourcePath: join(root, "scripts", "windows", "soty-managed-windows-reinstall.ps1")
      },
      {
        name: "prepare",
        fileName: "soty-prepare-windows-reinstall.ps1",
        sourcePath: join(root, "scripts", "windows", "soty-prepare-windows-reinstall.ps1")
      },
      {
        name: "arm",
        fileName: "soty-arm-windows-reinstall.ps1",
        sourcePath: join(root, "scripts", "windows", "soty-arm-windows-reinstall.ps1")
      },
      {
        name: "makeFastUsb",
        fileName: "soty-make-fast-usb.ps1",
        sourcePath: join(root, "scripts", "windows", "soty-make-fast-usb.ps1")
      }
    ]
  };
}
