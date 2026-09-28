import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { createPalette, contrastPairs, contrast, mix, normalizeTheme } from './theme/palette.mjs';

test('every brightness step preserves text, controls and focus contrast in both palettes', () => {
  let samples = 0;
  for (const scheme of ['light', 'dark']) for (let brightness = 0; brightness <= 100; brightness++) {
    for (const pair of contrastPairs(createPalette(scheme, brightness))) {
      samples++;
      assert.ok(pair.ratio >= pair.minimum, `${scheme} ${brightness}: ${pair.foreground}/${pair.background} = ${pair.ratio}`);
    }
  }
  assert.equal(samples, 11514);
});

test('default middle is a valid palette, malformed or old preferences have safe defaults', () => {
  assert.equal(contrast('#000000', '#ffffff'), 21);
  assert.ok(Math.abs(contrast('#777777', '#ffffff') - 4.478089453577214) < 1e-10);
  assert.deepEqual(normalizeTheme(), { themeMode: 'system', themeBrightness: 50 });
  assert.deepEqual(normalizeTheme({ themeMode: 'sepia', themeBrightness: NaN }), normalizeTheme());
  assert.equal(normalizeTheme({ themeMode: 'dark', themeBrightness: 102 }).themeBrightness, 100);
  assert.equal(normalizeTheme({ themeBrightness: -1 }).themeBrightness, 0);
  // Interpolating oppositely colored text and background across schemes would fail.
  assert.equal(contrast(mix('#000000', '#ffffff', .5), mix('#ffffff', '#000000', .5)), 1);
});

test('world styling consumes semantic colors and has no legacy personal or hex shape rules', () => {
  const postcss = createRequire(import.meta.resolve('vite'))('postcss');
  const ast = postcss.parse(readFileSync(new URL('./world.css', import.meta.url), 'utf8'));
  ast.walkRules(rule => {
    assert.doesNotMatch(rule.selector, /\.sw-my-grid|\.sw-my-cell|\.sw-personal-footer/);
    rule.walkDecls(decl => {
      if (/color|background|border|shadow|outline|fill|stroke/.test(decl.prop)) assert.doesNotMatch(decl.value, /#[\da-f]{3,8}\b|\b(?:white|black)\b|\brgba?\(/i, `${rule.selector}: ${decl.prop}`);
      if (/(?:^|[\s>])\.sw-(?:avatar|emblem|logo)(?:$|[:\s>,])/.test(rule.selector) && !/>\s*(?:span|img|\.sw-icon)/.test(rule.selector)) assert.notEqual(decl.prop, 'clip-path');
    });
  });
});
