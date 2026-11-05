import test, {before, after} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';

// Browser test IO only, matching avatar-context.browser.mjs. No Root runtime,
// signed identity, network fixture, persistent Chrome profile or new dependency.
const {chromium} = await import(process.env.SOTY_PLAYWRIGHT_MODULE || 'playwright');
const css = readFileSync(new URL('../../vendor/optical-glass/src/ui-kit.css', import.meta.url), 'utf8');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const actionSelector = '.glass-ui :is([data-glass-role="control"], [data-glass-role="accent"]):is(button, summary, [role="button"]):not(:where([data-glass-ignore], [data-glass-ignore] *))';

function withoutActionFloor(source) {
  assert.equal(source.split(actionSelector).length, 2, 'the control must remove exactly one production action rule');
  const start = source.indexOf(actionSelector), open = source.indexOf('{', start), end = source.indexOf('}', open) + 1;
  assert.equal(source.slice(open + 1, end - 1).replace(/\s+/gu, ' ').trim(),
    'min-inline-size: var(--glass-ui-control); min-block-size: var(--glass-ui-control);');
  // Leave every older material/descendant text reset and all other bytes intact.
  return source.slice(0, start) + source.slice(end);
}
const brokenCss = withoutActionFloor(css);
const glyph = '<svg aria-hidden="true" viewBox="0 0 22 22" width="22" height="22"><circle cx="11" cy="11" r="3" fill="currentColor"/></svg>';
const ids = ['summary-control', 'summary-accent', 'button-control', 'button-accent', 'role-button'];
let browser;

before(async () => { browser = await chromium.launch({channel: 'chrome', headless: true}); });
after(async () => { await browser?.close(); });

async function fixture(t, width, renderMode = 'static') {
  const page = await browser.newPage({viewport: {width, height: 850}});
  t.after(() => page.close());
  await page.setContent(`<!doctype html><html lang="ru"><meta charset="utf-8"><style>
    body{margin:12px}
    .material-card{width:min(100%,420px);padding:12px}
    .action-row{display:flex;align-items:center;gap:10px;margin-bottom:6px}
    .row-title{flex:1;min-width:0;overflow-wrap:anywhere}
    .action-menu{flex:none}
    .material-card .icon-action{display:grid;place-items:center;min-width:44px;min-height:44px;padding:0;list-style:none}
    .icon-action::-webkit-details-marker{display:none}
    .icon-action svg{width:20px;height:20px}
    .narrow-fields{display:flex;width:30px}
    .narrow-fields>:is(input,textarea){flex:1 1 0;inline-size:0}
    .small-card{inline-size:24px}
    .ignored-source{font:12px serif;color:rgb(11,22,33)}
    .source-action{display:grid;width:22px;height:23px;min-width:0;min-height:0;padding:0;border:0;background:rgb(203,211,219);font:inherit;color:inherit}
    .source-frame{width:240px;height:65px;border:0}
  </style>
  <main class="glass-ui" data-glass-material="working" data-glass-tone="light" data-glass-render-mode="${renderMode}">
    <section class="material-card" data-glass-role="card">
      ${['control', 'accent'].map(role => `<header class="action-row"><span class="row-title">Комната и обычный длинный заголовок</span>
        <details class="action-menu"><summary id="summary-${role}" class="icon-action" data-glass-role="${role}" aria-label="Действия ${role}">${glyph}</summary><div>Действия комнаты</div></details>
      </header>`).join('')}
      <div class="action-row">
        <button id="button-control" class="icon-action" data-glass-role="control" aria-label="Назад">${glyph}</button>
        <button id="button-accent" class="icon-action" data-glass-role="accent" aria-label="Слушать">${glyph}</button>
        <div id="role-button" class="icon-action" data-glass-role="control" role="button" tabindex="0" aria-label="Ещё">${glyph}</div>
      </div>
      <div class="narrow-fields"><input id="input" data-glass-role="field" aria-label="Текст" value="Сохранённый собственный текст"></div>
      <div class="narrow-fields"><textarea id="textarea" data-glass-role="field" aria-label="Черновик">Черновик</textarea></div>
      <section id="small-card" class="small-card" data-glass-role="card">Текст</section>
      <section data-glass-ignore class="ignored-source">
        <button id="ignored-button" class="source-action" data-glass-role="accent">+</button>
        <details><summary id="ignored-summary" class="source-action" data-glass-role="control">+</summary><p>Source content</p></details>
        <iframe class="source-frame" title="Independent Source" srcdoc='<!doctype html><style>body{margin:0}button{display:block;width:22px;height:23px;min-width:0;min-height:0;padding:0;border:0;color:rgb(11,22,33);font:12px serif}</style><button id="source-button" data-glass-role="accent">+</button>'></iframe>
      </section>
    </section>
  </main></html>`);
  return page;
}

async function measure(page) {
  return page.evaluate(ids => Object.fromEntries(ids.map(id => {
    const node = document.getElementById(id), rect = node.getBoundingClientRect(), style = getComputedStyle(node);
    const atCenter = document.elementFromPoint(rect.x + rect.width / 2, rect.y + rect.height / 2);
    return [id, {width: rect.width, height: rect.height, minInline: style.minInlineSize, minBlock: style.minBlockSize,
      centerHit: atCenter === node || node.contains(atCenter)}];
  })), ids);
}
function assertActionTargets(rows) {
  for (const id of ids) {
    const row = rows[id];
    assert.ok(row.width >= 44 && row.height >= 44, `${id}: 44px physical target required; observed ${row.width}×${row.height}`);
    assert.equal(row.minInline, '44px', `${id}: the shared action floor must win the nested material reset`);
    assert.equal(row.minBlock, '44px', `${id}: the shared block floor must remain explicit`);
    assert.equal(row.centerHit, true, `${id}: the visible center must hit that same action`);
  }
}

for (const width of [390, 1280]) {
  test(`${width}px: real Glass nested card actions retain 44×44 in optical and static DOM with native summary behavior`, async t => {
    for (const renderMode of ['optical', 'static']) {
      const page = await fixture(t, width, renderMode);
      await page.addStyleTag({content: css});
      const rows = await measure(page); assertActionTargets(rows);
      await page.locator('#summary-control').click();
      assert.equal(await page.locator('#summary-control').evaluate(node => node.parentElement.open), true);
      await page.locator('#summary-control').press('Enter');
      assert.equal(await page.locator('#summary-control').evaluate(node => node.parentElement.open), false);
      t.diagnostic(JSON.stringify({cssSha256: sha256(css), viewport: width, renderMode, rows}));
    }
  });

  test(`${width}px BROKEN: removing exactly the action floor reproduces 22px summary collapse under the SAME oracle`, async t => {
    for (const renderMode of ['optical', 'static']) {
      const page = await fixture(t, width, renderMode);
      await page.addStyleTag({content: brokenCss});
      const rows = await measure(page);
      // A 20px author glyph plus the shared working material's two border
      // pixels reproduces Root's 22px collapse in either CSS presentation.
      assert.equal(rows['summary-control'].width, 22, 'the control must reproduce icon-width collapse');
      assert.equal(rows['summary-control'].height, 44, 'the old defect is width collapse, not a hidden/zero-size fixture');
      assert.throws(() => assertActionTargets(rows), /summary-control: 44px physical target required/);
      t.diagnostic(JSON.stringify({mutantCssSha256: sha256(brokenCss), viewport: width, renderMode, rows}));
    }
  });

  test(`${width}px: field flex min0, ignored author controls and the real Source document remain independent`, async t => {
    const page = await fixture(t, width);
    const ownSource = () => page.locator('iframe').contentFrame().locator('#source-button').evaluate(node => {
      const r = node.getBoundingClientRect(), s = getComputedStyle(node);
      return {width: r.width, height: r.height, minInline: s.minInlineSize, minBlock: s.minBlockSize, color: s.color, font: s.font};
    });
    const ignored = () => page.locator('[data-glass-ignore]').evaluate(root => [...root.querySelectorAll('button,summary')].map(node => {
      const r = node.getBoundingClientRect(), s = getComputedStyle(node);
      return {id: node.id, width: r.width, height: r.height, minInline: s.minInlineSize, minBlock: s.minBlockSize, color: s.color, font: s.font};
    }));
    const beforeIgnored = await ignored(), beforeSource = await ownSource();
    await page.locator('#input').evaluate(node => {node.focus(); node.setSelectionRange(4, 13);});
    await page.addStyleTag({content: css});
    assert.deepEqual(await ignored(), beforeIgnored);
    assert.deepEqual(await ownSource(), beforeSource);
    const state = await page.evaluate(() => ({
      fields: ['input', 'textarea'].map(id => {const n = document.getElementById(id), r = n.getBoundingClientRect(); return {id, width: r.width, minInline: getComputedStyle(n).minInlineSize};}),
      cardWidth: document.getElementById('small-card').getBoundingClientRect().width,
      value: document.getElementById('input').value, start: document.getElementById('input').selectionStart, end: document.getElementById('input').selectionEnd,
    }));
    for (const field of state.fields) {assert.equal(field.minInline, '0px'); assert.ok(field.width > 0 && field.width <= 30, `${field.id} can shrink below the action floor`);}
    assert.equal(state.cardWidth, 24, 'ordinary material cards are not action targets');
    assert.equal(state.value, 'Сохранённый собственный текст'); assert.equal(state.start, 4); assert.equal(state.end, 13);
    t.diagnostic(JSON.stringify({viewport: width, state, ignored: beforeIgnored, source: beforeSource}));
  });
}

// A native ignored subtree must receive real pointers below a Glass container.
// Use the production uPlot implementation; no fake drag/click event dispatch.
const nativePlotCss = readFileSync(new URL('../../vendor/uplot/dist/uPlot.min.css', import.meta.url), 'utf8');
const nativePlotSource = readFileSync(new URL('../../vendor/uplot/dist/uPlot.esm.js', import.meta.url), 'utf8');
const nativePlotModule = 'data:text/javascript;base64,' + Buffer.from(nativePlotSource).toString('base64');
const nativeBoundaryCss = process.env.SOTY_GLASS_NATIVE_POINTER_CSS ? readFileSync(process.env.SOTY_GLASS_NATIVE_POINTER_CSS, 'utf8') : css;
for (const width of [390, 1280]) {
  test(`${width}px: ignored native subtree retains pointer click and real uPlot drag without overriding local pointer-none`, async t => {
    for (const renderMode of ['static', 'optical']) {
      const page = await fixture(t, width, renderMode);
      // Specific component rules precede Glass, so load order cannot conceal
      // a reset that wrongly overrides its own disabled/overlay policy.
      await page.addStyleTag({content: '.native-off,.component-off{pointer-events:none}.native-off,.component-off{height:20px}.native-plot{width:100%}'});
      await page.addStyleTag({content: nativePlotCss});
      await page.addStyleTag({content: nativeBoundaryCss});
      await page.evaluate(async moduleUrl => {
        document.querySelector('.material-card').setAttribute('data-glass-ui-hit', 'container');
        const root = document.querySelector('.ignored-source');
        root.insertAdjacentHTML('beforeend', `<div id="local-off" data-glass-ignore class="native-off">off</div>
          <div id="inline-off" data-glass-ignore style="pointer-events:none;height:20px">off</div>
          <div id="child-off" class="component-off">off</div>
          <div id="ignored-glass-root" class="glass-ui" data-glass-ignore><button id="native-root-button" class="source-action">+</button></div>
          <div id="native-chart" class="native-plot" data-glass-ignore tabindex="0"></div>`);
        const {default: uPlot} = await import(moduleUrl);
        const state = window.__glassNative = {buttonClicks: 0, rootClicks: 0, chartDowns: 0};
        document.getElementById('ignored-button').addEventListener('click', () => state.buttonClicks++);
        document.getElementById('native-root-button').addEventListener('click', () => state.rootClicks++);
        const host = document.getElementById('native-chart');
        const x = Array.from({length: 61}, (_, i) => 1700000000 + 60 * i), y = x.map((_, i) => 20 + i % 11);
        state.plot = new uPlot({width: Math.floor(host.getBoundingClientRect().width), height: 220,
          legend: {show: false}, series: [{}, {stroke: 'blue'}],
          cursor: {drag: {x: true, y: false, setScale: true}}, scales: {x: {time: true}}}, [x, y], host);
        state.plot.over.addEventListener('mousedown', () => state.chartDowns++);
      }, nativePlotModule);
      const read = () => page.evaluate(() => {
        const pointer = selector => getComputedStyle(document.querySelector(selector)).pointerEvents;
        const plot = window.__glassNative.plot, r = plot.over.getBoundingClientRect();
        const hit = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2);
        return {nativePointer: pointer('.ignored-source'), nativeRootPointer: pointer('#ignored-glass-root'),
          localOffPointer: pointer('#local-off'), inlineOffPointer: pointer('#inline-off'), childOffPointer: pointer('#child-off'),
          ordinaryPointer: pointer('#button-control'), containerPointer: pointer('.material-card'),
          overPointer: getComputedStyle(plot.over).pointerEvents,
          selectPointer: getComputedStyle(plot.root.querySelector('.u-select')).pointerEvents,
          cursorPointers: [...plot.root.querySelectorAll('.u-cursor-x,.u-cursor-y,.u-cursor-pt')].map(n => getComputedStyle(n).pointerEvents),
          overlayHit: hit === plot.over || plot.over.contains(hit), span: plot.scales.x.max - plot.scales.x.min,
          buttonClicks: window.__glassNative.buttonClicks, rootClicks: window.__glassNative.rootClicks, chartDowns: window.__glassNative.chartDowns};
      });
      for (const id of ['ignored-button', 'native-root-button']) {
        await page.locator('#' + id).scrollIntoViewIfNeeded();
        const r = await page.locator('#' + id).boundingBox();
        await page.mouse.click(r.x + r.width / 2, r.y + r.height / 2);
      }
      await page.locator('#native-chart').scrollIntoViewIfNeeded();
      const initial = await read(), over = await page.locator('#native-chart .u-over').boundingBox();
      const y = over.y + over.height / 2, start = over.x + over.width * .2, end = over.x + over.width * .72;
      await page.mouse.move(start, y); await page.mouse.down(); await page.mouse.move(end, y, {steps: 8});
      const selectionWidth = await page.locator('#native-chart .u-select').evaluate(node => node.getBoundingClientRect().width);
      await page.mouse.up();
      const zoomed = await read();
      await page.mouse.dblclick(over.x + over.width / 2, y);
      const reset = await read();
      t.diagnostic(JSON.stringify({cssSha256: sha256(nativeBoundaryCss), width, renderMode, initial, selectionWidth, zoomed, reset}));
      assert.equal(initial.nativePointer, 'auto', 'ignored native boundary must stop inherited Glass pointer suppression');
      assert.equal(initial.nativeRootPointer, 'auto', 'ignored Glass root has the same native boundary contract');
      assert.equal(initial.overPointer, 'auto'); assert.equal(initial.overlayHit, true, 'real uPlot overlay must own its visible hit');
      assert.equal(zoomed.buttonClicks, 1, 'native button receives its pointer click');
      assert.equal(zoomed.rootClicks, 1, 'native button below ignored Glass root receives its click');
      assert.equal(zoomed.chartDowns, 1, 'uPlot receives the drag mousedown');
      assert.ok(selectionWidth > 20, 'real uPlot drag creates a nonzero native selection');
      assert.ok(zoomed.span < initial.span * .8, 'drag narrows the real uPlot x scale');
      assert.equal(reset.span, initial.span, 'native double-click resets zoom');
      assert.equal(reset.localOffPointer, 'none', 'specific author pointer-none wins even when loaded before Glass');
      assert.equal(reset.inlineOffPointer, 'none', 'inline author pointer-none remains');
      assert.equal(reset.childOffPointer, 'none', 'boundary reset does not force descendants to auto');
      assert.equal(reset.selectPointer, 'none', 'uPlot selection remains click-through');
      assert.ok(reset.cursorPointers.length > 0 && reset.cursorPointers.every(value => value === 'none'), 'uPlot cursors retain pointer-none');
      assert.equal(reset.ordinaryPointer, 'auto', 'host control restoration remains');
      assert.equal(reset.containerPointer, 'none', 'Glass container click-through remains');
    }
  });
}
