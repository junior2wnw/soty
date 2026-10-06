// Manual fixture generator, never imported by the application. No microphone,
// camera, account, remote origin or user data is read. Open the printed loopback
// page in Chrome, click Generate and commit the small synthetic fixtures.
import { createServer } from 'node:http';
import { mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
const folder = fileURLToPath(new URL('../fixtures/', import.meta.url));
const names = new Set(['chrome.png', 'chrome.jpg', 'chrome.webp', 'chrome-recorder.webm']);
const page = `<!doctype html><meta charset="utf-8"><title>Synthetic media fixtures</title>
<button id="generate">Generate synthetic fixtures</button><pre id="status">Ready</pre><script>
const encode = async blob => { const bytes = new Uint8Array(await blob.arrayBuffer()); let text = ''; for(const byte of bytes) text += String.fromCharCode(byte); return btoa(text); };
document.getElementById('generate').onclick = async () => {
 const status = document.getElementById('status'); status.textContent='Recording synthetic audio';
 try {
  const canvas=document.createElement('canvas'); canvas.width=64; canvas.height=48;
  const context=canvas.getContext('2d'); context.fillStyle='#456789'; context.fillRect(0,0,64,48); context.fillStyle='#ffd54f'; context.fillRect(8,8,24,24);
  const files={};
  for(const [name,type] of [['chrome.png','image/png'],['chrome.jpg','image/jpeg'],['chrome.webp','image/webp']]) files[name]=await encode(await new Promise(resolve=>canvas.toBlob(resolve,type,0.8)));
  const audio=new AudioContext({sampleRate:48000}); await audio.resume();
  const destination=audio.createMediaStreamDestination(), oscillator=audio.createOscillator(); oscillator.frequency.value=440; oscillator.connect(destination);
  const recorder=new MediaRecorder(destination.stream,{mimeType:'audio/webm;codecs=opus'}), chunks=[];
  const stopped=new Promise(resolve=>recorder.onstop=resolve); recorder.ondataavailable=event=>{if(event.data.size)chunks.push(event.data)};
  recorder.start(100); oscillator.start(); await new Promise(resolve=>setTimeout(resolve,350)); recorder.stop(); await stopped; oscillator.stop(); await audio.close();
  files['chrome-recorder.webm']=await encode(new Blob(chunks,{type:recorder.mimeType}));
  const response=await fetch('/fixtures',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({files,userAgent:navigator.userAgent,mimeType:recorder.mimeType})});
  if(!response.ok)throw new Error('Fixture write failed'); status.textContent='Fixtures written';
 }catch(error){status.textContent='Failed: '+error.message;}
};</script>`;
const server = createServer(async (req, res) => {
  if (req.url === '/' && req.method === 'GET') { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(page); return; }
  if (req.url !== '/fixtures' || req.method !== 'POST') { res.writeHead(404).end(); return; }
  try {
    let count = 0; const chunks = [];
    for await (const chunk of req) { count += chunk.length; if (count > 1048576) throw Error('fixture_request_limit'); chunks.push(chunk); }
    const { files, userAgent, mimeType } = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (!files || Object.keys(files).length !== names.size || Object.keys(files).some(name => !names.has(name))) throw Error('fixture_names');
    mkdirSync(folder, { recursive: true });
    for (const [name, base64] of Object.entries(files)) {
      const bytes = Buffer.from(base64, 'base64'); if (bytes.toString('base64') !== base64) throw Error('fixture_base64');
      writeFileSync(folder + name, bytes, { flag: 'wx' });
    }
    writeFileSync(folder + 'chrome-provenance.json', JSON.stringify({ synthetic: true, source: 'Canvas + AudioContext oscillator + MediaRecorder',
      audio: { sampleRate: 48000, frequencyHz: 440, requestedDurationMs: 350, timesliceMs: 100, mimeType }, userAgent }, null, 2) + '\n', { flag: 'wx' });
    res.end('ok'); process.stdout.write('fixtures-written\n'); server.close();
  } catch { res.writeHead(400).end('fixture_failed'); }
});
server.listen(0, '127.0.0.1', () => process.stdout.write(`http://127.0.0.1:${server.address().port}/\n`));
