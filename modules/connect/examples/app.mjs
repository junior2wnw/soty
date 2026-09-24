import { createConnectClient } from '../browser/index.mjs';
import { openConnectPanel, parseConnectLink } from '../ui/index.mjs';
const client = createConnectClient({ projectId: 'example', endpoint: '/api/connect/rpc' });
const show = () => openConnectPanel({ client, productName: 'Другой проект', label: 'Мой профиль', initialIntent: parseConnectLink(location.href) });
document.querySelector('#profile').addEventListener('click', show);
if (parseConnectLink(location.href)) show();
