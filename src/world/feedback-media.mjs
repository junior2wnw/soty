// Browser-neutral media lifecycle adapted from EcoLab capture/useRecorder.
// There is no CRM role, DOM scraping, transcript or provider call here.
export const FEEDBACK_MEDIA_LIMITS = Object.freeze({ totalAttachmentBytes: 1048576, maxAttachments: 3, maxAudioSeconds: 120 });

export function attachmentBytes(value) {
  const encoded = value?.dataBase64;
  if (typeof encoded !== 'string' || !encoded.length || encoded.length > 1398104 || encoded.length % 4 !== 0 ||
      !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)) throw new Error('invalid_feedback_attachment');
  try { if (btoa(atob(encoded)) !== encoded) throw new Error(); } catch { throw new Error('invalid_feedback_attachment'); }
  return encoded.length * 3 / 4 - (encoded.endsWith('==') ? 2 : encoded.endsWith('=') ? 1 : 0);
}

export function validateAttachmentBudget(attachments, limits = FEEDBACK_MEDIA_LIMITS) {
  if (!Array.isArray(attachments) || attachments.length > limits.maxAttachments) throw new Error('feedback_attachment_count');
  let bytes = 0;
  for (const value of attachments) {
    if (!value || !['image', 'audio'].includes(value.kind) || typeof value.name !== 'string' || value.name.length > 120 ||
        !(value.kind === 'image' ? ['image/png', 'image/jpeg', 'image/webp'] : ['audio/webm', 'audio/ogg']).includes(value.mimeType)) throw new Error('invalid_feedback_attachment');
    bytes += attachmentBytes(value);
  }
  if (bytes > limits.totalAttachmentBytes) throw new Error('feedback_attachment_bytes');
  return bytes;
}

export function safeRasterSize(width, height, maxSide = 1440) {
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 1 || height < 1 || width > 8192 || height > 8192 || width * height > 16777216) throw new Error('feedback_image_dimensions');
  const scale = Math.min(1, maxSide / Math.max(width, height));
  return { width: Math.max(1, Math.round(width * scale)), height: Math.max(1, Math.round(height * scale)) };
}

export function rasterFileDimensions(bytes, mimeType) {
  const data = new Uint8Array(bytes), view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const same = (offset, value) => value.every((byte, index) => data[offset + index] === byte);
  let width, height;
  if (mimeType === 'image/png' && data.length >= 24 && same(0, [137, 80, 78, 71, 13, 10, 26, 10]) && same(12, [73, 72, 68, 82])) {
    width = view.getUint32(16); height = view.getUint32(20);
  } else if (mimeType === 'image/jpeg' && same(0, [255, 216])) {
    let offset = 2;
    while (offset + 4 <= data.length) {
      if (data[offset++] !== 255) break; while (data[offset] === 255) offset++;
      const marker = data[offset++]; if (marker === 217 || marker === 218) break;
      if (marker >= 208 && marker <= 215 || marker === 1) continue;
      if (offset + 2 > data.length) break; const length = view.getUint16(offset);
      if (length < 2 || offset + length > data.length) break;
      if ([192, 193, 194, 195, 197, 198, 199, 201, 202, 203, 205, 206, 207].includes(marker) && length >= 8) {
        height = view.getUint16(offset + 3); width = view.getUint16(offset + 5); break;
      }
      offset += length;
    }
  } else if (mimeType === 'image/webp' && data.length >= 30 && same(0, [82, 73, 70, 70]) && same(8, [87, 69, 66, 80])) {
    let offset = 12;
    while (offset + 8 <= data.length) {
      const length = view.getUint32(offset + 4, true), start = offset + 8;
      if (start + length > data.length) break;
      if (same(offset, [86, 80, 56, 88]) && length >= 10) {
        width = 1 + data[start + 4] + (data[start + 5] << 8) + (data[start + 6] << 16);
        height = 1 + data[start + 7] + (data[start + 8] << 8) + (data[start + 9] << 16); break;
      }
      if (same(offset, [86, 80, 56, 76]) && length >= 5 && data[start] === 47) {
        width = 1 + data[start + 1] + ((data[start + 2] & 63) << 8);
        height = 1 + (data[start + 2] >> 6) + (data[start + 3] << 2) + ((data[start + 4] & 15) << 10); break;
      }
      if (same(offset, [86, 80, 56, 32]) && length >= 10 && same(start + 3, [157, 1, 42])) {
        width = view.getUint16(start + 6, true) & 16383; height = view.getUint16(start + 8, true) & 16383; break;
      }
      offset = start + length + (length & 1);
    }
  }
  if (width === undefined || height === undefined) throw new Error('feedback_image_format');
  safeRasterSize(width, height); return { width, height };
}

export function rasterSelection(rect, width, height) {
  const values = [rect?.x, rect?.y, rect?.width, rect?.height];
  if (!values.every(Number.isFinite) || rect.width <= 0 || rect.height <= 0) throw new Error('feedback_image_selection');
  const x = Math.max(0, Math.min(width - 1, Math.floor(rect.x))), y = Math.max(0, Math.min(height - 1, Math.floor(rect.y)));
  const right = Math.max(x + 1, Math.min(width, Math.ceil(rect.x + rect.width))), bottom = Math.max(y + 1, Math.min(height, Math.ceil(rect.y + rect.height)));
  return { x, y, width: right - x, height: bottom - y };
}

function aborted(signal) { if (signal?.aborted) throw new DOMException('Cancelled', 'AbortError'); }
function readData(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader(); reader.onload = () => resolve(String(reader.result).split(',')[1]);
    reader.onerror = () => reject(new Error('feedback_media_read')); reader.readAsDataURL(blob);
  });
}
export async function blobAttachment(blob, kind, name, signal) {
  aborted(signal);
  const dataBase64 = await readData(blob); aborted(signal);
  return { kind, name, mimeType: blob.type.split(';')[0], dataBase64 };
}
const canvasBlob = (canvas, type, quality) => new Promise((resolve, reject) => canvas.toBlob(value => value ? resolve(value) : reject(new Error('feedback_image_export')), type, quality));

async function exportCanvas(canvas, maxBytes, signal) {
  // The exported raster is the attachment; edits never preserve a hidden original.
  let output = canvas;
  for (let attempt = 0; attempt < 5; attempt++) {
    aborted(signal);
    const png = await canvasBlob(output, 'image/png');
    if (png.size <= maxBytes) return blobAttachment(png, 'image', 'screen.png', signal);
    const jpeg = await canvasBlob(output, 'image/jpeg', 0.86);
    if (jpeg.size <= maxBytes) return blobAttachment(jpeg, 'image', 'screen.jpg', signal);
    const smaller = document.createElement('canvas'); smaller.width = Math.max(1, Math.round(output.width * 0.75)); smaller.height = Math.max(1, Math.round(output.height * 0.75));
    smaller.getContext('2d').drawImage(output, 0, 0, smaller.width, smaller.height); output = smaller;
  }
  throw new Error('feedback_attachment_bytes');
}

export async function rasterFile(file, maxBytes, signal) {
  if (!['image/png', 'image/jpeg', 'image/webp'].includes(file.type) || file.size > 8 * 1024 * 1024) throw new Error('feedback_image_format');
  aborted(signal);
  rasterFileDimensions(await file.arrayBuffer(), file.type); aborted(signal);
  const bitmap = await createImageBitmap(file);
  try {
    aborted(signal);
    const size = safeRasterSize(bitmap.width, bitmap.height), canvas = document.createElement('canvas');
    canvas.width = size.width; canvas.height = size.height; canvas.getContext('2d').drawImage(bitmap, 0, 0, size.width, size.height);
    return await exportCanvas(canvas, maxBytes, signal);
  } finally { bitmap.close(); }
}

export async function editRaster(attachment, rect, operation, maxBytes, signal) {
  if (attachment.kind !== 'image' || !['crop', 'redact'].includes(operation)) throw new Error('feedback_image_selection');
  const raw = Uint8Array.from(atob(attachment.dataBase64), value => value.charCodeAt(0));
  const bitmap = await createImageBitmap(new Blob([raw], { type: attachment.mimeType }));
  try {
    aborted(signal); safeRasterSize(bitmap.width, bitmap.height);
    const selection = rasterSelection(rect, bitmap.width, bitmap.height), canvas = document.createElement('canvas');
    canvas.width = operation === 'crop' ? selection.width : bitmap.width; canvas.height = operation === 'crop' ? selection.height : bitmap.height;
    const context = canvas.getContext('2d');
    if (operation === 'crop') context.drawImage(bitmap, selection.x, selection.y, selection.width, selection.height, 0, 0, selection.width, selection.height);
    else { context.drawImage(bitmap, 0, 0); context.fillStyle = '#17212b'; context.fillRect(selection.x, selection.y, selection.width, selection.height); }
    return await exportCanvas(canvas, maxBytes, signal);
  } finally { bitmap.close(); }
}

export async function captureSelectedDisplay(maxBytes, signal) {
  if (!navigator.mediaDevices?.getDisplayMedia) throw new Error('feedback_capture_unavailable');
  // Called directly from a gesture. The browser, not the shell, selects a surface.
  const stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
  const stop = () => stream.getTracks().forEach(track => track.stop());
  signal?.addEventListener('abort', stop, { once: true });
  const video = document.createElement('video'); video.muted = true; video.srcObject = stream;
  try {
    aborted(signal); await video.play(); aborted(signal);
    const size = safeRasterSize(video.videoWidth, video.videoHeight), canvas = document.createElement('canvas');
    canvas.width = size.width; canvas.height = size.height; canvas.getContext('2d').drawImage(video, 0, 0, size.width, size.height);
    return await exportCanvas(canvas, maxBytes, signal);
  } finally { stop(); video.srcObject = null; signal?.removeEventListener('abort', stop); }
}

export function createFeedbackRecorder({ maxBytes, maxSeconds = 120, signal, onTick = () => {} }) {
  let stream = null, recorder = null, timer, cancelled = false, ended = false, rejectResult;
  const stopTracks = () => { stream?.getTracks().forEach(track => track.stop()); stream = null; clearInterval(timer); };
  const cancel = () => { cancelled = true; if (recorder?.state === 'recording') recorder.stop(); stopTracks(); rejectResult?.(new DOMException('Cancelled', 'AbortError')); };
  signal?.addEventListener('abort', cancel, { once: true });
  async function start() {
    if (!navigator.mediaDevices?.getUserMedia || typeof MediaRecorder === 'undefined'
      || typeof MediaRecorder.isTypeSupported !== 'function') throw new Error('feedback_record_unavailable');
    aborted(signal);
    // Match the actual server's bounded Opus profile before requesting a mic.
    const mime = ['audio/webm;codecs=opus', 'audio/ogg;codecs=opus'].find(value => MediaRecorder.isTypeSupported(value));
    if (!mime) throw new Error('feedback_record_unavailable');
    try {
      stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      if (cancelled || signal?.aborted) { stopTracks(); throw new DOMException('Cancelled', 'AbortError'); }
      recorder = new MediaRecorder(stream, { mimeType: mime, audioBitsPerSecond: 48000 });
      return await new Promise((resolve, reject) => {
        rejectResult = reject; const parts = []; let bytes = 0, seconds = 0, overflow = false;
        recorder.ondataavailable = event => { if (!event.data.size || cancelled) return; bytes += event.data.size;
          if (bytes > maxBytes) { overflow = true; if (recorder.state === 'recording') recorder.stop(); } else parts.push(event.data); };
        recorder.onerror = () => { stopTracks(); reject(new Error('feedback_record_failed')); };
        recorder.onstop = async () => {
          stopTracks(); ended = true;
          try { if (cancelled || signal?.aborted) throw new DOMException('Cancelled', 'AbortError');
            if (overflow) throw new Error('feedback_attachment_bytes'); if (!parts.length) throw new Error('feedback_record_empty');
            resolve(await blobAttachment(new Blob(parts, { type: mime.split(';')[0] }), 'audio', 'voice.' + mime.split('/')[1].split(';')[0], signal));
          } catch (error) { reject(error); }
        };
        recorder.start(500); onTick(0);
        timer = setInterval(() => { onTick(++seconds); if (seconds >= maxSeconds && recorder.state === 'recording') recorder.stop(); }, 1000);
      });
    } finally { ended = true; stopTracks(); signal?.removeEventListener('abort', cancel); }
  }
  return { start, stop() { if (recorder?.state === 'recording') recorder.stop(); }, cancel, active: () => !ended && !!stream };
}
