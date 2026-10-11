/** Explicit user click only. No private reads, server calls or automatic transfer. */
export function copyVisibleText({ text, write, isCurrent, onStatus, success, failure }: {
  text: string; write?: ((text: string) => Promise<void>) | undefined; isCurrent(): boolean;
  onStatus(message: string): void; success: string; failure: string;
}): Promise<boolean> {
  if (!text || !isCurrent()) return Promise.resolve(false);
  try {
    if (!write) throw new Error("clipboard_unavailable");
    // Start synchronously in the click, preserving mobile user activation.
    return Promise.resolve(write(text)).then(() => {
      if (!isCurrent()) return false;
      onStatus(success); return true;
    }, () => { if (isCurrent()) onStatus(failure); return false; });
  } catch { if (isCurrent()) onStatus(failure); return Promise.resolve(false); }
}
