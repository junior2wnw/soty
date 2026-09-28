import { mkdirSync, writeFileSync } from 'node:fs';
import { deflateSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';
import { HEX_FLOWER, axialToPixel, hexPolygonPoints, pointInHex } from '../src/geometry/hex.mjs';

// Code-native brand artwork, using the same regular hexagon as the UI.
const directory = fileURLToPath(new URL('../public/icons/', import.meta.url));
mkdirSync(directory, { recursive: true });
const background = '#303a31', colors = ['#e6ad4a', '#c9d7bd', '#c9d7bd', '#c9d7bd', '#c9d7bd', '#c9d7bd', '#c9d7bd'];
const radius = 52, cells = HEX_FLOWER.map((point, index) => ({ ...axialToPixel(point, radius, 12), color: colors[index] }));
const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><rect width="512" height="512" fill="${background}"/>${cells.map(cell => `<polygon points="${hexPolygonPoints(radius, { x: 256 + cell.x, y: 256 + cell.y })}" fill="${cell.color}"/>`).join('')}</svg>\n`;
writeFileSync(`${directory}soty.svg`, svg);
const rgb = hex => hex.slice(1).match(/../g).map(value => parseInt(value, 16));
const backdrop = rgb(background); cells.forEach(cell => { cell.rgb = rgb(cell.color); });
const table = Array.from({ length: 256 }, (_, value) => { for (let i = 0; i < 8; i++) value = value & 1 ? 0xedb88320 ^ value >>> 1 : value >>> 1; return value >>> 0; });
const chunk = (name, data) => {
  const body = Buffer.concat([Buffer.from(name), data]); let crc = 0xffffffff;
  for (const byte of body) crc = table[(crc ^ byte) & 255] ^ crc >>> 8;
  const head = Buffer.alloc(4), tail = Buffer.alloc(4); head.writeUInt32BE(data.length); tail.writeUInt32BE((crc ^ 0xffffffff) >>> 0);
  return Buffer.concat([head, body, tail]);
};
for (const size of [180, 192, 512]) {
  const data = Buffer.alloc(size * (1 + size * 3));
  for (let y = 0; y < size; y++) for (let x = 0; x < size; x++) {
    const color = [0, 0, 0];
    for (let sy = 0; sy < 4; sy++) for (let sx = 0; sx < 4; sx++) {
      const point = { x: (x + (sx + .5) / 4) * 512 / size - 256, y: (y + (sy + .5) / 4) * 512 / size - 256 };
      const pixel = cells.find(cell => pointInHex({ x: point.x - cell.x, y: point.y - cell.y }, radius))?.rgb ?? backdrop;
      for (let c = 0; c < 3; c++) color[c] += pixel[c];
    }
    for (let c = 0; c < 3; c++) data[y * (1 + size * 3) + 1 + x * 3 + c] = Math.round(color[c] / 16);
  }
  const header = Buffer.alloc(13); header.writeUInt32BE(size, 0); header.writeUInt32BE(size, 4); header[8] = 8; header[9] = 2;
  const png = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', header), chunk('IDAT', deflateSync(data)), chunk('IEND', Buffer.alloc(0))]);
  writeFileSync(`${directory}soty-${size}.png`, png);
  console.log(`soty-${size}.png ${png.length} bytes`);
}
