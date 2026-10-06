#!/usr/bin/env node
import { openSync, readSync, closeSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { ContractError, LIMITS, parseContractJson, validateDescriptor, contractDigest, createAdmissionHost, planAdmission, materializeAuthorDraft } from './index.mjs';
import { createAuthorDraft } from './sdk.mjs';

const help = Object.freeze({
  ok: true, prototype: true, productionAdmission: false,
  message: 'В проверенном контексте автор задаёт название. Платформа подставляет явно одобренный профиль из настроенного registry.',
  commands: [
    'draft --title НАЗВАНИЕ',
    'materialize DRAFT_FILE --fixture-host HOST_FILE',
    'validate DESCRIPTOR_FILE',
    'plan DESCRIPTOR_FILE --host HOST_FILE --request-id REQUEST_ID'
  ],
  hint: 'materialize и --host используют только локальную синтетическую конфигурацию; файл не доказывает владение.'
});
const errorHelp = Object.freeze({
  cli_arguments: ['Команда или аргументы не распознаны.', 'Запустите --help; используйте указанную форму команды.'],
  input_not_found: ['Файл не найден.', 'Проверьте выбранный локальный файл.'],
  input_unreadable: ['Файл нельзя прочитать.', 'Выберите доступный обычный файл.'],
  invalid_json: ['JSON повреждён или содержит недопустимую конструкцию.', 'Исправьте синтаксис JSON; используйте целые безопасные числа.'],
  invalid_utf8: ['Файл не является корректным UTF-8.', 'Сохраните файл в UTF-8.'],
  duplicate_key: ['JSON содержит повторяющееся поле.', 'Удалите повтор, включая эквивалентные escaped ключи.'],
  input_limit: ['Вход превышает допустимые размеры или глубину.', 'Сократите документ; пределы указаны в README.'],
  unsafe_number: ['Число не входит в безопасный целочисленный профиль.', 'Используйте безопасные целые числа без -0, дробей и экспонент.'],
  closed_fields: ['Набор полей не соответствует закрытому контракту.', 'Для простого начала используйте draft только с названием; advanced descriptor сверяйте с README.'],
  unsupported_schema: ['Версия схемы не поддерживается.', 'Используйте указанную v1; версия не преобразуется автоматически.'],
  unsupported_version: ['Версия ссылки недопустима.', 'Укажите положительную поддерживаемую версию проверенного контракта.'],
  invalid_title: ['Название пустое или слишком длинное.', 'Укажите название от 1 до 160 символов.'],
  invalid_string: ['Строка содержит недопустимые символы.', 'Удалите управляющие символы и исправьте Unicode.'],
  secret_not_allowed: ['В metadata обнаружен возможный секрет.', 'Удалите credentials из metadata; используйте отдельный credential broker.'],
  author_profile_required: ['Доверенный профиль автора не настроен.', 'Оператор должен явно выбрать named authorProfile; автор не копирует host pins.'],
  capability_digest_mismatch: ['Semantic digest не совпадает с контрактом функции.', 'Изменение контракта требует новой версии и отдельного рассмотрения binding.'],
  binding_missing: ['Проверенный binding не найден.', 'Оператор должен зарегистрировать точную версию обработчика; фабрика не выдаёт права.'],
  binding_contract_mismatch: ['Binding не допускает этот контракт функции.', 'Согласуйте новую semantic версию; не расширяйте прежние effects или recipients.'],
  admission_request_limit: ['Локальный host достиг лимита admission requests.', 'Существующий request можно повторить; не обходите лимит сбросом idempotency history.'],
  host_closed: ['Локальный host уже закрыт.', 'Закрытый fixture нельзя использовать для callbacks.'],
  authority_changed: ['Доверенная конфигурация изменилась.', 'Получите текущий host context; старые callbacks не принимаются.'],
  audience_privacy: ['Публичная отправка запрещена для частного приложения.', 'Используйте допуск участников и private feedback profile.'],
  feedback_provider_missing: ['Feedback provider pin не одобрен.', 'Оператор должен выбрать зарегистрированную точную ссылку.'],
  feedback_profile_missing: ['Capture или retention profile pin не одобрен.', 'Оператор должен выбрать соответствующие зарегистрированные profiles.']
});
function at(stage, operation) {
  try { return operation(); }
  catch (error) {
    const code = error instanceof ContractError ? error.code
      : stage === 'read' ? (error?.code === 'ENOENT' ? 'input_not_found' : 'input_unreadable') : 'cli_failed';
    const safe = new ContractError(code);
    safe.stage = error instanceof ContractError && ['arguments', 'read', 'parse', 'validate', 'host', 'materialize', 'plan'].includes(error.stage) ? error.stage : stage;
    throw safe;
  }
}
export function cliError(error) {
  const code = error instanceof ContractError && /^[a-z_]{1,60}$/.test(error.code) ? error.code : 'cli_failed';
  const [message, hint] = errorHelp[code] ?? ['Контракт не прошёл проверку.', 'Сверьте закрытый профиль и trusted references с README.'];
  const stage = ['arguments', 'read', 'parse', 'validate', 'host', 'materialize', 'plan'].includes(error?.stage) ? error.stage : 'internal';
  return { ok: false, error: code, stage, message, hint };
}

function read(path) {
  const buffer = at('read', () => {
    const fd = openSync(path, 'r'), bytes = Buffer.alloc(LIMITS.bytes + 1);
    let size = 0;
    try {
      while (size < bytes.length) { const count = readSync(fd, bytes, size, bytes.length - size, null); if (!count) break; size += count; }
    } finally { closeSync(fd); }
    if (size > LIMITS.bytes) throw new ContractError('input_limit');
    return bytes.subarray(0, size);
  });
  return at('parse', () => parseContractJson(buffer));
}
export function runCli(args) {
  if (args.length === 1 && args[0] === '--help') return help;
  const [mode, filename, ...flags] = args;
  at('arguments', () => {
    if (mode === 'draft') {
      if (args.length !== 3 || filename !== '--title') throw new ContractError('cli_arguments');
    } else if (mode === 'materialize') {
      if (!filename || flags.length !== 2 || flags[0] !== '--fixture-host') throw new ContractError('cli_arguments');
    } else if (mode === 'validate') {
      if (!filename || flags.length) throw new ContractError('cli_arguments');
    } else if (mode === 'plan') {
      if (!filename || flags.length !== 4 || flags[0] !== '--host' || flags[2] !== '--request-id') throw new ContractError('cli_arguments');
    } else throw new ContractError('cli_arguments');
  });
  if (mode === 'draft') return at('validate', () => createAuthorDraft({ title: flags[0] }));
  if (mode === 'materialize') {
    const draft = at('validate', () => createAuthorDraft(read(filename)));
    const host = at('host', () => createAdmissionHost(read(flags[1])));
    return { ok: true, prototype: true, productionAdmission: false, trustedInput: 'local-fixture-only',
      descriptor: at('materialize', () => materializeAuthorDraft(host, draft)) };
  }
  const descriptor = at('validate', () => validateDescriptor(read(filename)));
  if (mode === 'validate') {
    return { ok: true, prototype: true, productionAdmission: false, schema: descriptor.schema,
      digest: contractDigest(descriptor), capabilities: descriptor.capabilities.length, feedback: 'required', reviews: descriptor.reviews.mode };
  }
  const host = at('host', () => createAdmissionHost(read(flags[1])));
  return { ok: true, plan: at('plan', () => planAdmission(host, descriptor, flags[3])) };
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { process.stdout.write(JSON.stringify(runCli(process.argv.slice(2))) + '\n'); }
  catch (error) {
    process.stderr.write(JSON.stringify(cliError(error)) + '\n'); process.exitCode = 1;
  }
}
