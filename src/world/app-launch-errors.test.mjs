import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';

function readDiagnostic(source) {
  const ast = ts.createSourceFile('app-stage.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const node = ast.statements.find(value => ts.isFunctionDeclaration(value) && value.name?.text === 'appLaunchFailure');
  assert.ok(node);
  const code = ts.transpileModule(node.getText(ast).replace(/^export /, ''),
    { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
  return vm.runInNewContext(code + '\nappLaunchFailure;');
}
const diagnostic = readDiagnostic(readFileSync(new URL('./app-stage.ts', import.meta.url), 'utf8'));
const distinctCauses = read => assert.notEqual(read({ code: 'apps_sessions_busy' }).title, read({ code: 'apps_entry_busy' }).title);

test('capacity and an unconfirmed entry no longer hide behind the same busy diagnosis', () => {
  // Frozen busy branch from31d70930; source archives deliberately contain no Git.
  const old = error => {
    if (['apps_launch_busy', 'apps_entry_busy', 'apps_sessions_busy', 'app_public_capacity', 'app_capacity'].includes(error.code))
      return { title: 'Приложение сейчас занято', detail: 'Попробуйте ещё раз немного позже.' };
    return { title: 'Не удалось открыть приложение', detail: 'Проверьте подключение и попробуйте ещё раз.' };
  };
  assert.throws(() => distinctCauses(old), assert.AssertionError, 'the same oracle detects the original ambiguous diagnosis');
  distinctCauses(diagnostic);
});

test('retirement failure offers retry while authority and offline errors keep their distinct messages', () => {
  assert.match(diagnostic({ code: 'app_host_retirement_pending' }).detail, /Повторите проверку/u);
  assert.match(diagnostic({ code: 'app_host_retirement_unavailable' }).detail, /хранилище/u);
  assert.equal(diagnostic({ code: 'ACTIVE_PROFILE_CHANGED' }).title, 'Аккаунт изменился');
  assert.equal(diagnostic({ code: 'app_offline' }).title, 'Устройство не в сети');
  assert.notEqual(diagnostic({ code: 'apps_access_denied' }).title, diagnostic({ code: 'apps_sessions_busy' }).title);
});
