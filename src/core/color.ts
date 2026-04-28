const palette = [
  "#0f6bff",
  "#d0352b",
  "#087a4a",
  "#8b3ffc",
  "#c15b00",
  "#007c89",
  "#b0005a",
  "#4b6f00",
  "#2451a6",
  "#6f4a00"
];

export function colorFor(value: string): string {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return palette[Math.abs(hash) % palette.length] ?? "#0f6bff";
}

export function safeColor(value: string | undefined, fallbackSeed: string): string {
  return value && /^#[\da-f]{6}$/iu.test(value) ? value : colorFor(fallbackSeed);
}
