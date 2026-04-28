export function clock(value = new Date()): string {
  return value.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit"
  });
}
