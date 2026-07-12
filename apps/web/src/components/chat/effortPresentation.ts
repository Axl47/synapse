// FILE: effortPresentation.ts
// Purpose: Centralizes the rare effort states that need distinct visual emphasis.

export const ULTRA_EFFORT_TEXT_CLASS_NAME = "text-violet-300 dark:text-violet-300";

export function isUltraEffort(value: string | null | undefined): boolean {
  return value?.trim().toLowerCase() === "ultra";
}
