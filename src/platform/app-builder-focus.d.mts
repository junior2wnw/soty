export interface AssistantFocusClaim { anchor: Element; area: HTMLElement; revision: number; displaced: boolean }
export interface AssistantFocusHandoff {
  observe(element: Element | null): void;
  releaseOutside(target: Node | null): void;
  capture(area?: HTMLElement): AssistantFocusClaim | null;
  restore(claim: AssistantFocusClaim | null, fallback: HTMLElement): boolean;
}
export function createAssistantFocusHandoff(options: { root: HTMLElement; getActiveElement(): Element | null; isNeutral(element: Element | null): boolean }): AssistantFocusHandoff;
