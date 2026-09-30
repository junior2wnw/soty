import { canonicalHash } from '../../server/validation.mjs';
import { BUILTIN_DOCUMENTATION } from '../../server/documentation.mjs';

// Explicit fixture configuration, never a production documentation fallback.
// No invented input/output examples: actual sample validation has its own tests.
export function fixtureDocumentation(catalog) {
  return catalog.filter(entry => entry.visibility === 'public').map(entry => {
    const { executionEnabled: _operational, ...contract } = entry;
    const contractDigest = canonicalHash({ ...contract, resources: [...entry.resources].sort(),
      effects: [...entry.effects].sort(), recipients: [...entry.recipients].sort() });
    const builtin = BUILTIN_DOCUMENTATION.find(item => item.capabilityId === entry.capabilityId
      && item.version === entry.version && item.contractDigest === contractDigest);
    return builtin || { capabilityId: entry.capabilityId, version: entry.version, contractDigest,
      locales: {
        ru: { title: 'Тестовая функция', summary: 'Контракт для проверки сервиса.', useWhen: [], notFor: [], examples: [] },
        en: { title: 'Test capability', summary: 'Service acceptance fixture.', useWhen: [], notFor: [], examples: [] },
      }, keywords: { ru: [], en: [] } };
  });
}
