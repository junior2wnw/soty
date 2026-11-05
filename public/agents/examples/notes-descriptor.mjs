import { createDescriptor } from '../sdk/app-contract/sdk.mjs';

// Pass the real registered app's current source and feedback refs from its owner inspection.
export function notesDescriptor({ app, feedback, binding, workspace }) {
  const namespace = app.namespace;
  return createDescriptor({ app, feedback, capabilities: [{ id: `${namespace}:notes.read`, version: 1, title: 'Read notes',
    inputSchema: { type: 'object', properties: { limit: { type: 'integer', minimum: 1, maximum: 20 } }, required: ['limit'], additionalProperties: false },
    outputSchema: { type: 'object', properties: { notes: { type: 'array', maxItems: 20, items: { type: 'string', maxLength: 1000 } } }, required: ['notes'], additionalProperties: false },
    resources: [workspace], effects: ['read'], recipients: [workspace], binding }] });
}
