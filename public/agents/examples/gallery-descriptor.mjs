import { createDescriptor } from '../sdk/app-contract/sdk.mjs';

export function galleryDescriptor({ app, feedback, binding, collection }) {
  return createDescriptor({ app, feedback, capabilities: [{ id: `${app.namespace}:gallery.count`, version: 1, title: 'Count selected photos',
    inputSchema: { type: 'object', properties: {}, required: [], additionalProperties: false },
    outputSchema: { type: 'object', properties: { count: { type: 'integer', minimum: 0, maximum: 100000 } }, required: ['count'], additionalProperties: false },
    resources: [collection], effects: ['read'], recipients: [collection], binding }] });
}
