import type { IconName } from "../icons";

export type RuntimeModuleTarget = "agent" | "apps" | "actions" | "access" | "qr" | "files" | "chess";

export type RuntimeModuleDefinition = {
  readonly id: RuntimeModuleTarget;
  readonly title: string;
  readonly summary: string;
  readonly icon: IconName;
  readonly target: RuntimeModuleTarget;
  readonly entityScoped: boolean;
};

export const runtimeModuleDefinitions = [
  { id: "agent", title: "Агент", summary: "помощник", icon: "agent", target: "agent", entityScoped: true },
  { id: "apps", title: "Приложения", summary: "инструменты", icon: "apps", target: "apps", entityScoped: true },
  { id: "actions", title: "Действия", summary: "быстрый запуск", icon: "check", target: "actions", entityScoped: true },
  { id: "access", title: "Доступ", summary: "устройства", icon: "shield", target: "access", entityScoped: true },
  { id: "qr", title: "QR", summary: "подключение", icon: "qr", target: "qr", entityScoped: false },
  { id: "files", title: "Файлы", summary: "передача", icon: "clip", target: "files", entityScoped: true },
  { id: "chess", title: "Игра", summary: "шахматы", icon: "chess", target: "chess", entityScoped: true }
] as const satisfies readonly RuntimeModuleDefinition[];

export function runtimeModuleTargetFromString(value: string): RuntimeModuleTarget | "" {
  const target = value.trim().toLowerCase();
  return runtimeModuleDefinitions.some((module) => module.target === target)
    ? target as RuntimeModuleTarget
    : "";
}

export function runtimeModuleUsesEntity(target: RuntimeModuleTarget): boolean {
  return runtimeModuleDefinitions.find((module) => module.target === target)?.entityScoped === true;
}
