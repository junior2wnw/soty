import { useEffect, useState, type FormEvent } from 'react';
import { Copy, Eye, EyeOff, KeyRound, Trash2 } from 'lucide-react';
import type { PlannerSnapshot } from '../../shared/types';
import { request } from '../api';

type Key = {
  id: string;
  name: string;
  workspaceIds: string[];
  readOnly: boolean;
  expiresAt: string;
  lastUsedAt?: string | null;
};
type Connection = { path: string; stdio: { command: string; args: string[] } };
export default function AgentSettings({
  snapshot,
  workspaceId,
}: {
  snapshot: PlannerSnapshot;
  workspaceId: string;
}) {
  const [keys, setKeys] = useState<Key[]>([]);
  const [connection, setConnection] = useState<Connection | null>(null);
  const [name, setName] = useState('Мой агент');
  const [readOnly, setReadOnly] = useState(true);
  const [days, setDays] = useState(30);
  const [token, setToken] = useState('');
  const [visible, setVisible] = useState(false);
  const [transport, setTransport] = useState<'http' | 'stdio'>('http');
  const [error, setError] = useState('');
  const [status, setStatus] = useState('');
  const [busy, setBusy] = useState(false);
  const owner = snapshot.memberships.some(
    (m) => m.workspaceId === workspaceId && m.userId === snapshot.user.id && m.role === 'owner',
  );
  const workspace = snapshot.workspaces.find((w) => w.id === workspaceId);
  useEffect(() => {
    let active = true;
    setToken('');
    setVisible(false);
    setError('');
    setStatus('');
    setKeys([]);
    request<{ keys: Key[]; connection: Connection }>('/api/agent/keys')
      .then((result) => {
        if (active) {
          setKeys(result.keys);
          setConnection(result.connection);
        }
      })
      .catch((e) => {
        if (active) setError(e.message);
      });
    return () => {
      active = false;
    };
  }, [workspaceId, snapshot.user.id]);
  async function create(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError('');
    setStatus('');
    setToken('');
    setVisible(false);
    try {
      const result = await request<Key & { token: string }>('/api/agent/keys', 'POST', {
        name,
        workspaceIds: [workspaceId],
        readOnly,
        expiresInDays: days,
      });
      setToken(result.token);
      const { token: _, ...key } = result;
      setKeys((previous) => [key, ...previous]);
      setStatus('Ключ создан. Скопируйте подключение сейчас — секрет показывается один раз.');
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function revoke(keyId: string) {
    setBusy(true);
    setError('');
    try {
      await request(`/api/agent/keys/${encodeURIComponent(keyId)}`, 'DELETE');
      setKeys((previous) => previous.filter((k) => k.id !== keyId));
      setToken('');
      setVisible(false);
      setStatus('Ключ отозван. Доступ агента прекращён.');
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  const endpoint = `${window.location.origin}/mcp`;
  const config = JSON.stringify(
    {
      mcpServers: {
        planner:
          transport === 'http'
            ? { url: endpoint, headers: { Authorization: `Bearer ${token}` } }
            : {
                ...connection?.stdio,
                env: { PLANNER_URL: window.location.origin, PLANNER_MCP_TOKEN: token },
              },
      },
    },
    null,
    2,
  );
  async function copy(value: string) {
    try {
      await navigator.clipboard.writeText(value);
      setStatus('Подключение скопировано. Вставьте его в настройки MCP своего агента.');
    } catch {
      setError('Копирование недоступно. Покажите подключение и скопируйте вручную.');
    }
  }
  return (
    <>
      <section className="settings-section">
        <h3>
          <KeyRound size={18} /> Агенты и MCP
        </h3>
        <p className="helper">
          Агент видит выбранное пространство и меняет шкалу через те же проверки, что и вы.
          Настройки других пространств ему недоступны.
        </p>
        <label>
          Адрес MCP
          <input value={endpoint} readOnly aria-label="Адрес MCP" />
        </label>
        <button type="button" className="secondary-button" onClick={() => void copy(endpoint)}>
          <Copy size={15} />
          Скопировать адрес
        </button>
      </section>
      <form className="settings-section" onSubmit={create}>
        <h3>Новое подключение к «{workspace?.name}»</h3>
        <div className="form-grid">
          <label>
            Название агента
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              maxLength={100}
            />
          </label>
          <label>
            Что разрешено
            <select
              value={readOnly ? 'read' : 'edit'}
              onChange={(e) => setReadOnly(e.target.value === 'read')}
            >
              <option value="read">Только чтение</option>
              <option value="edit">Чтение и изменение</option>
            </select>
          </label>
          <label>
            Срок доступа
            <select value={days} onChange={(e) => setDays(Number(e.target.value))}>
              <option value={1}>1 день</option>
              <option value={7}>7 дней</option>
              <option value={30}>30 дней</option>
              <option value={90}>90 дней</option>
            </select>
          </label>
        </div>
        {!owner && <p className="helper">Выдавать ключи может владелец пространства.</p>}
        <button className="primary-button" disabled={busy || !owner}>
          Создать ключ
        </button>
      </form>
      {token && (
        <section className="settings-section">
          <h3>Подключите агента</h3>
          <label>
            Транспорт
            <select
              value={transport}
              onChange={(e) => setTransport(e.target.value as 'http' | 'stdio')}
            >
              <option value="http">HTTP — по адресу MCP</option>
              <option value="stdio">Stdio — на этом компьютере</option>
            </select>
          </label>
          <p className="helper">
            {transport === 'http'
              ? 'Для MCP клиентов с Streamable HTTP. Ключ ограничен выбранным пространством.'
              : 'Для локального MCP клиента на этом компьютере. Запускается через Node.js из папки планировщика.'}
          </p>
          <div className="agent-config-actions">
            <button type="button" className="primary-button" onClick={() => void copy(config)}>
              <Copy size={15} />
              Скопировать подключение
            </button>
            <button
              type="button"
              className="secondary-button"
              onClick={() => setVisible((v) => !v)}
            >
              {visible ? <EyeOff size={15} /> : <Eye size={15} />} {visible ? 'Скрыть' : 'Показать'}
            </button>
          </div>
          {visible ? (
            <textarea
              className="agent-config"
              aria-label="Конфигурация MCP"
              value={config}
              readOnly
              rows={transport === 'http' ? 10 : 16}
            />
          ) : (
            <p className="helper">Секрет скрыт. Кнопка копирует готовое подключение целиком.</p>
          )}
        </section>
      )}
      <section className="settings-section">
        <h3>Выданные ключи</h3>
        {keys
          .filter((k) => k.workspaceIds.includes(workspaceId))
          .map((k) => (
            <div key={k.id} className="agent-key-row">
              <div>
                <strong>{k.name}</strong>
                <small>
                  {k.readOnly ? 'Только чтение' : 'Чтение и изменение'} · до{' '}
                  {new Date(k.expiresAt).toLocaleDateString('ru-RU')}
                  {Date.parse(k.expiresAt) < Date.now() ? ' · истёк' : ''}
                </small>
              </div>
              <button
                type="button"
                className="icon-button"
                aria-label={`Отозвать ключ ${k.name}`}
                onClick={() => void revoke(k.id)}
                disabled={busy}
              >
                <Trash2 size={16} />
              </button>
            </div>
          ))}
        {!keys.some((k) => k.workspaceIds.includes(workspaceId)) && (
          <p className="helper">Подключений пока нет.</p>
        )}
      </section>
      <section className="settings-section">
        <h3>Понятные действия</h3>
        <p className="helper">
          Создать поездку с перелётами, настроить свои типы и поля, добавить повторения и
          оповещения, перенести цепочку с просмотром последствий. Агент может начать с инструмента
          planner_help — в нём есть готовые примеры.
        </p>
      </section>
      {error && (
        <p className="inline-error" role="alert">
          {error}
        </p>
      )}
      {status && (
        <p className="inline-success" role="status">
          {status}
        </p>
      )}
    </>
  );
}
