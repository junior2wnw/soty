import { test } from 'node:test';
import assert from 'node:assert/strict';
import { attachmentFileName, attachmentDisposition, attachmentDownloadHeaders,
  ATTACHMENT_NAME_MAX_BYTES } from '../attachment-file-policy.mjs';

test('keeps Russian, NFC, spaces and complete emoji names for display and download', () => {
  assert.equal(attachmentFileName('  Сме\u0301та 👨‍👩‍👧‍👦 №15.pdf  '), 'Сме\u0301та 👨‍👩‍👧‍👦 №15.pdf'.normalize('NFC'));
  const header = attachmentDisposition('План дома.pdf');
  assert.match(header, /^attachment; filename="file.pdf"; filename\*=UTF-8''/u);
  assert.equal(decodeURIComponent(header.split("UTF-8''")[1]), 'План дома.pdf');
});

test('strips supplied paths, CRLF header injection and bidi extension disguises', () => {
  for (const source of ['../../report.txt', 'C:\\private\\report.txt']) assert.equal(attachmentFileName(source), 'report.txt');
  const name = attachmentFileName('photo\u202egpj.exe\r\nX-Evil: 1');
  assert.equal(name, 'photogpj.exeX-Evil 1');
  assert.doesNotMatch(attachmentDisposition('x\r\nSet-Cookie: bad'), /[\r\n\u202e]/u);
});

test('neutralizes Windows reserved devices including superscript variants', () => {
  for (const source of ['CON', 'aux.txt', 'LPT9.pdf', 'COM¹.txt', 'NUL. ', 'con...', 'CONIN$', 'CONOUT$', 'conin$.txt']) {
    assert.ok(attachmentFileName(source).startsWith('_'));
  }
  assert.equal(attachmentFileName('console.txt'), 'console.txt');
  for (const source of ['', '.', '..', '/', '\\', '~']) assert.equal(attachmentFileName(source), 'файл');
});

test('names have a bounded UTF-8 size without a split emoji or lost short extension', () => {
  const family = '👨‍👩‍👧‍👦';
  const result = attachmentFileName(family.repeat(80) + '.pdf');
  assert.ok(Buffer.byteLength(result) <= ATTACHMENT_NAME_MAX_BYTES);
  assert.ok(result.endsWith('.pdf'));
  assert.equal(result.slice(0, -4), family.repeat(result.slice(0, -4).length / family.length));
  assert.ok(Buffer.byteLength(attachmentFileName('я'.repeat(1000))) <= ATTACHMENT_NAME_MAX_BYTES);
});

test('ill-formed Unicode cannot break Content-Disposition encoding', () => {
  const header = attachmentDisposition('a\ud800b\udc00.pdf');
  assert.equal(decodeURIComponent(header.split("UTF-8''")[1]), 'a\ufffdb\ufffd.pdf');
});

test('a single over-budget combining sequence retains a useful stem and extension', () => {
  assert.equal(attachmentFileName('a' + '\u0301'.repeat(500) + '.pdf'), 'файл.pdf');
  assert.equal(attachmentFileName('a' + '\u0301'.repeat(500)), 'файл');
});

test('emoji flags, modifiers, tags and ZWJ names preserve complete defined units', () => {
  for (const unit of ['🇷🇺', '👍🏽', '🏴\u{e0067}\u{e0062}\u{e007f}', '👨‍👩‍👧‍👦']) {
    const name = attachmentFileName(unit.repeat(80) + '.pdf'), stem = name.slice(0, -4);
    assert.equal(stem, unit.repeat(stem.length / unit.length));
    assert.ok(Buffer.byteLength(name) <= ATTACHMENT_NAME_MAX_BYTES);
  }
});

test('legacy fallback has no percent decoding, quotes, path or header escape', () => {
  for (const input of ['100%20real.txt', "who's (draft)*.txt", 'a";x=y.txt', 'имя.svg']) {
    const header = attachmentDisposition(input), quoted = /filename="([^"]+)"/u.exec(header)[1];
    assert.match(quoted, /^[A-Za-z0-9 _().+-]+$/u);
    assert.doesNotMatch(header, /[\r\n]/u);
    assert.equal(decodeURIComponent(header.split("UTF-8''")[1]), attachmentFileName(input));
  }
});

test('unknown and active formats remain downloads with no sniffing or inline claim', () => {
  for (const name of ['page.html', 'drawing.svg', 'file.exe', 'document.pdf']) {
    const headers = attachmentDownloadHeaders(name);
    assert.equal(headers['content-type'], 'application/octet-stream');
    assert.equal(headers['x-content-type-options'], 'nosniff');
    assert.ok(headers['content-disposition'].startsWith('attachment;'));
    assert.equal(headers['cache-control'], 'private, no-store');
    assert.ok(Object.isFrozen(headers));
  }
});

test('policy is deterministic and idempotent for hostile bounded name corpus', () => {
  for (const prefix of ['CON', '../', '\u202e', '\u0000', 'x'.repeat(200), '💼'.repeat(150)]) {
    for (const suffix of ['.txt', '.svg', ' .', '\r\n"', 'COM1']) {
      const name = attachmentFileName(prefix + suffix);
      assert.equal(attachmentFileName(name), name);
      assert.ok(Buffer.byteLength(name) <= ATTACHMENT_NAME_MAX_BYTES);
      assert.doesNotMatch(name, /[\p{Cc}\\/:<>"|?*\u202a-\u202e]/u);
    }
  }
});

test('oversize inputs and objects are rejected without invoking accessors', () => {
  assert.throws(() => attachmentFileName('x'.repeat(4097)), /attachment_filename_invalid/u);
  assert.throws(() => attachmentFileName({ toString() { throw Error('must not invoke'); } }), /attachment_filename_invalid/u);
});
