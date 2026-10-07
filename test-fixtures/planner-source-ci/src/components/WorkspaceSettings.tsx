import { useState, type FormEvent } from 'react';
import { DateTime } from 'luxon';
import {
  Bell,
  Bot,
  Blocks,
  Copy,
  Download,
  Eye,
  EyeOff,
  FileUp,
  Globe2,
  Layers3,
  Plus,
  Save,
  ShieldCheck,
  Trash2,
  Users,
  Workflow,
  X,
} from 'lucide-react';
import type { EntityKind, FieldDefinition, PlannerSnapshot, Role } from '../../shared/types';
import { request, type Invitation, type Mutate } from '../api';
import { canEdit, dateLabel, kindLabels, roleLabels, uid } from '../utils';
import Modal from './Modal';
import AgentSettings from './AgentSettings';

type Props = {
  snapshot: PlannerSnapshot;
  workspaceId: string;
  mutate: Mutate;
  busy: boolean;
  onClose: () => void;
  onWorkspace: (id: string) => void;
  refresh: () => Promise<void>;
};
const panels = [
  { id: 'spaces', title: 'Пространства', icon: Globe2 },
  { id: 'team', title: 'Команда', icon: Users },
  { id: 'types', title: 'Типы и поля', icon: Blocks },
  { id: 'resources', title: 'Ресурсы', icon: Layers3 },
  { id: 'rules', title: 'Автоматика', icon: Workflow },
  { id: 'notifications', title: 'Оповещения', icon: Bell },
  { id: 'data', title: 'Данные', icon: FileUp },
  { id: 'agents', title: 'Агенты и MCP', icon: Bot },
  { id: 'account', title: 'Учётная запись', icon: ShieldCheck },
];

export default function WorkspaceSettings({
  snapshot,
  workspaceId,
  mutate,
  busy,
  onClose,
  onWorkspace,
  refresh,
}: Props) {
  const [panel, setPanel] = useState('spaces');
  const [currentId, setCurrentId] = useState(
    workspaceId === 'all' ? (snapshot.workspaces[0]?.id ?? '') : workspaceId,
  );
  const current = snapshot.workspaces.find((w) => w.id === currentId);
  const editable = !!current && canEdit(snapshot, current.id);
  const owner =
    snapshot.memberships.find((m) => m.workspaceId === currentId && m.userId === snapshot.user.id)
      ?.role === 'owner';
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [spaceName, setSpaceName] = useState('');
  const [spaceMode, setSpaceMode] = useState<'personal' | 'team'>('personal');
  const [spaceZone, setSpaceZone] = useState('Asia/Yekaterinburg');
  const [memberName, setMemberName] = useState('');
  const [memberEmail, setMemberEmail] = useState('');
  const [memberRole, setMemberRole] = useState<Role>('editor');
  const [invitation, setInvitation] = useState<Invitation | null>(null);
  const [invitationVisible, setInvitationVisible] = useState(false);
  const [invitationCopied, setInvitationCopied] = useState(false);
  const [invitationBusy, setInvitationBusy] = useState(false);
  const [invitationToken, setInvitationToken] = useState('');
  const [typeName, setTypeName] = useState('');
  const [typeKind, setTypeKind] = useState<EntityKind>('period');
  const [typeColor, setTypeColor] = useState('#6f8f7b');
  const [fields, setFields] = useState<FieldDefinition[]>([]);
  const [resourceName, setResourceName] = useState('');
  const [resourceKind, setResourceKind] = useState('person');
  const [capacity, setCapacity] = useState(1);
  const [unit, setUnit] = useState('чел.');
  const [weekdays, setWeekdays] = useState([1, 2, 3, 4, 5]);
  const [ruleName, setRuleName] = useState('');
  const [trigger, setTrigger] = useState('before-start');
  const [lead, setLead] = useState(60);
  const [ruleAction, setRuleAction] = useState('signal');
  const [followup, setFollowup] = useState('');
  const [ruleType, setRuleType] = useState('');
  const [notificationPreferences, setNotificationPreferences] = useState(
    snapshot.settings.notifications,
  );
  const [format, setFormat] = useState<'json' | 'csv' | 'ics'>('json');
  const [importText, setImportText] = useState('');
  const [importName, setImportName] = useState('');
  const [exporting, setExporting] = useState(false);
  const [templateName, setTemplateName] = useState('');
  const [templateSource, setTemplateSource] = useState('');
  const [authMode, setAuthMode] = useState('register');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState(snapshot.user.displayName);
  async function act(path: string, method = 'POST', body?: unknown, message = 'Сохранено') {
    setError('');
    setSuccess('');
    try {
      const next = await mutate(path, method, body);
      setSuccess(message);
      return next;
    } catch (e) {
      setError((e as Error).message);
      return null;
    }
  }
  async function createSpace(event: FormEvent) {
    event.preventDefault();
    const previous = new Set(snapshot.workspaces.map((w) => w.id));
    const next = await act('/api/workspaces', 'POST', {
      name: spaceName,
      mode: spaceMode,
      timezone: spaceZone,
    });
    const created = next?.workspaces.find((w) => !previous.has(w.id));
    if (created) {
      setCurrentId(created.id);
      onWorkspace(created.id);
      setSpaceName('');
    }
  }
  async function createType(event: FormEvent) {
    event.preventDefault();
    const next = await act('/api/types', 'POST', {
      workspaceId: currentId,
      label: typeName,
      kind: typeKind,
      color: typeColor,
      icon: typeKind,
      fields,
    });
    if (next) {
      setTypeName('');
      setFields([]);
    }
  }
  async function createResource(event: FormEvent) {
    event.preventDefault();
    const next = await act('/api/resources', 'POST', {
      workspaceId: currentId,
      name: resourceName,
      kind: resourceKind,
      capacity,
      unit,
      timezone: current?.timezone ?? spaceZone,
      workingWeekdays: weekdays,
    });
    if (next) setResourceName('');
  }
  async function createRule(event: FormEvent) {
    event.preventDefault();
    const next = await act('/api/rules', 'POST', {
      workspaceId: currentId,
      name: ruleName,
      enabled: true,
      trigger,
      leadMinutes: lead,
      action: ruleAction,
      ...(ruleType ? { typeId: ruleType } : {}),
      ...(ruleAction === 'create-followup'
        ? { followupTitle: followup, ownerId: snapshot.user.id }
        : {}),
    });
    if (next) setRuleName('');
  }
  function showInvitation(next: Invitation) {
    setInvitation(next);
    setInvitationVisible(false);
    setInvitationCopied(false);
  }
  async function createMember(event: FormEvent) {
    event.preventDefault();
    const next = await act(
      '/api/memberships',
      'POST',
      { workspaceId: currentId, displayName: memberName, email: memberEmail, role: memberRole },
      'Приглашение создано',
    );
    if (next) {
      setMemberEmail('');
      setMemberName('');
      if (next.invitation) showInvitation(next.invitation);
    }
  }
  async function renewInvitation(userId: string) {
    setError('');
    setSuccess('');
    setInvitationBusy(true);
    try {
      showInvitation(
        await request<Invitation>('/api/invitations', 'POST', { workspaceId: currentId, userId }),
      );
      setSuccess('Новый код создан. Прежний код больше не действует.');
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setInvitationBusy(false);
    }
  }
  async function copyInvitation() {
    if (!invitation) return;
    try {
      await navigator.clipboard.writeText(invitation.token);
      setInvitationCopied(true);
    } catch {
      setError('Браузер не разрешил копирование. Покажите код и скопируйте его вручную.');
    }
  }
  async function acceptInvitation(event: FormEvent) {
    event.preventDefault();
    const before = new Set(snapshot.workspaces.map((w) => w.id));
    const next = await act(
      '/api/invitations/accept',
      'POST',
      { invitationToken: invitationToken.trim() },
      'Доступ к пространству получен',
    );
    if (next) {
      setInvitationToken('');
      const added = next.workspaces.find((w) => !before.has(w.id));
      if (added) setCurrentId(added.id);
      onWorkspace('all');
    }
  }
  async function exportData() {
    setError('');
    setSuccess('');
    setExporting(true);
    try {
      const response = await fetch(
        `/api/export?workspaceId=${encodeURIComponent(currentId)}&format=${format}`,
        { credentials: 'same-origin' },
      );
      if (!response.ok) {
        const failure = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(failure.error || 'Не удалось экспортировать данные');
      }
      const warningsHeader = response.headers.get('X-Planner-Warnings');
      let warnings: string[] = [];
      if (warningsHeader) {
        try {
          const decoded: unknown = JSON.parse(decodeURIComponent(warningsHeader));
          if (Array.isArray(decoded))
            warnings = decoded.filter((warning): warning is string => typeof warning === 'string');
        } catch {
          /* The download is still valid when optional metadata is unavailable. */
        }
      }
      const file = await response.blob();
      const url = URL.createObjectURL(file);
      const link = document.createElement('a');
      link.href = url;
      link.download = `planner-${currentId}.${format}`;
      document.body.append(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      setSuccess(
        warnings.length
          ? `Файл экспорта подготовлен. ${warnings.join(' ')}`
          : 'Файл экспорта подготовлен',
      );
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setExporting(false);
    }
  }
  async function importData(event: FormEvent) {
    event.preventDefault();
    const next = await act(
      '/api/import',
      'POST',
      { workspaceId: currentId, format, content: importText },
      'Данные импортированы',
    );
    const entry = next?.audit
      .filter((a) => a.action === 'import' && a.workspaceId === currentId)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0];
    const after = entry?.after as { warnings?: unknown } | undefined;
    const reported = next?.importWarnings ?? after?.warnings;
    if (Array.isArray(reported)) {
      const warnings = reported.filter((warning): warning is string => typeof warning === 'string');
      if (warnings.length) setSuccess(`Данные импортированы. ${warnings.join(' ')}`);
    }
  }
  async function saveTemplate(event: FormEvent) {
    event.preventDefault();
    const root = snapshot.entities.find((e) => e.id === templateSource);
    if (!root) return;
    const source = snapshot.entities.filter((e) => e.id === root.id || isChildOf(e.id, root.id));
    const first = DateTime.fromISO(
      root.plan.start ?? source.find((e) => e.plan.start)?.plan.start ?? snapshot.serverTime,
    );
    const items = source.map((e) => ({
      key: e.id,
      ...(e.parentId && source.some((p) => p.id === e.parentId) ? { parentKey: e.parentId } : {}),
      title: e.title,
      typeId: e.typeId,
      kind: e.kind,
      offsetDays: e.plan.start ? DateTime.fromISO(e.plan.start).diff(first, 'days').days : 0,
      durationDays:
        e.plan.start && e.plan.end
          ? DateTime.fromISO(e.plan.end).diff(DateTime.fromISO(e.plan.start), 'days').days
          : 0,
      fields: e.fields,
      description: e.description,
      schedule: e.plan,
      allocations: e.allocations,
      recurrence: e.recurrence,
      dueAt: e.dueAt,
      tags: e.tags,
    }));
    const dependencies = snapshot.dependencies
      .filter((d) => source.some((e) => e.id === d.fromId) && source.some((e) => e.id === d.toId))
      .map((d) => ({ fromKey: d.fromId, toKey: d.toId, kind: d.kind, lagMinutes: d.lagMinutes }));
    const next = await act('/api/templates', 'POST', {
      workspaceId: currentId,
      name: templateName,
      description: `Из процесса «${root.title}»`,
      anchorDate: first.toISO(),
      timezone: snapshot.workspaces.find((w) => w.id === currentId)?.timezone ?? root.plan.timezone,
      items,
      dependencies,
    });
    if (next) {
      setTemplateName('');
      setTemplateSource('');
    }
  }
  function isChildOf(id: string, parentId: string) {
    let e = snapshot.entities.find((e) => e.id === id);
    const seen = new Set<string>();
    while (e?.parentId && !seen.has(e.id)) {
      seen.add(e.id);
      if (e.parentId === parentId) return true;
      e = snapshot.entities.find((p) => p.id === e!.parentId);
    }
    return false;
  }
  async function saveNotifications(event: FormEvent) {
    event.preventDefault();
    let enabled = notificationPreferences.browserEnabled;
    if (enabled) {
      if (!('Notification' in window)) {
        setError('Этот браузер не поддерживает оповещения. Оповещения внутри шкалы доступны.');
        return;
      }
      const permission =
        Notification.permission === 'default'
          ? await Notification.requestPermission()
          : Notification.permission;
      if (permission !== 'granted') {
        enabled = false;
        setError('Браузер не разрешил оповещения. Измените разрешение сайта в браузере.');
      }
    }
    setNotificationPreferences((p) => ({ ...p, browserEnabled: enabled }));
    await act('/api/settings', 'PATCH', {
      notifications: { ...notificationPreferences, browserEnabled: enabled },
    });
  }
  async function authenticate(event: FormEvent) {
    event.preventDefault();
    const result = await act(
      `/api/auth/${authMode}`,
      'POST',
      authMode === 'register'
        ? {
            email,
            password,
            displayName,
            claimLocal: snapshot.user.local === true,
            ...(invitationToken.trim() ? { invitationToken: invitationToken.trim() } : {}),
          }
        : { email, password },
      authMode === 'register' ? 'Учётная запись создана' : 'Вход выполнен',
    );
    if (result) {
      setPassword('');
      setInvitationToken('');
      setCurrentId(result.workspaces[0]?.id ?? '');
      onWorkspace('all');
    }
  }
  return (
    <Modal
      title="Настроить своё время"
      subtitle="Личная жизнь и работа могут жить рядом — с отдельными участниками и правами."
      onClose={onClose}
      wide
    >
      <div className="settings-nav" role="tablist" aria-label="Настройки">
        {panels.map((p) => (
          <button
            key={p.id}
            role="tab"
            aria-selected={panel === p.id}
            className={panel === p.id ? 'active' : ''}
            onClick={() => {
              setPanel(p.id);
              setError('');
              setSuccess('');
              setInvitation(null);
              setInvitationVisible(false);
            }}
          >
            <p.icon size={16} />
            {p.title}
          </button>
        ))}
      </div>
      <div className="form-body settings-body">
        {!['spaces', 'notifications', 'account'].includes(panel) && (
          <label className="workspace-setting-select">
            Пространство
            <select
              value={currentId}
              onChange={(e) => {
                setCurrentId(e.target.value);
                setSuccess('');
                setInvitation(null);
                setInvitationVisible(false);
              }}
            >
              {snapshot.workspaces.map((w) => (
                <option key={w.id} value={w.id}>
                  {w.name}
                </option>
              ))}
            </select>
          </label>
        )}
        {panel === 'agents' && <AgentSettings snapshot={snapshot} workspaceId={currentId} />}
        {panel === 'spaces' && (
          <>
            <div className="workspace-cards">
              {snapshot.workspaces.map((w) => (
                <button
                  className={currentId === w.id ? 'workspace-card active' : 'workspace-card'}
                  key={w.id}
                  onClick={() => {
                    setCurrentId(w.id);
                    onWorkspace(w.id);
                  }}
                >
                  <span className="workspace-icon">
                    <Globe2 size={20} />
                  </span>
                  <strong>{w.name}</strong>
                  <small>
                    {w.mode === 'personal' ? 'Личное' : 'Команда'} · {w.timezone}
                  </small>
                  <span>
                    {
                      roleLabels[
                        snapshot.memberships.find(
                          (m) => m.workspaceId === w.id && m.userId === snapshot.user.id,
                        )?.role ?? 'viewer'
                      ]
                    }
                  </span>
                </button>
              ))}
            </div>
            <form className="settings-section" onSubmit={createSpace}>
              <h3>Новое пространство</h3>
              <div className="form-grid">
                <label>
                  Название
                  <input
                    value={spaceName}
                    onChange={(e) => setSpaceName(e.target.value)}
                    required
                    placeholder="Жизнь, команда, путешествия…"
                  />
                </label>
                <label>
                  Для кого
                  <select
                    value={spaceMode}
                    onChange={(e) => setSpaceMode(e.target.value as 'personal' | 'team')}
                  >
                    <option value="personal">Для себя</option>
                    <option value="team">Для команды</option>
                  </select>
                </label>
                <label>
                  Часовой пояс
                  <input
                    value={spaceZone}
                    onChange={(e) => setSpaceZone(e.target.value)}
                    required
                  />
                </label>
                <button className="primary-button form-bottom" disabled={busy}>
                  <Plus size={15} />
                  Создать
                </button>
              </div>
            </form>
            {current && owner && (
              <form
                className="settings-section"
                onSubmit={(event) => {
                  event.preventDefault();
                  const form = new FormData(event.currentTarget);
                  void act(`/api/workspaces/${currentId}`, 'PATCH', {
                    name: form.get('name'),
                    description: form.get('description'),
                    timezone: form.get('timezone'),
                  });
                }}
                key={currentId}
              >
                <h3>Настройки «{current.name}»</h3>
                <div className="form-grid">
                  <label>
                    Название
                    <input name="name" defaultValue={current.name} required />
                  </label>
                  <label>
                    Часовой пояс
                    <input name="timezone" defaultValue={current.timezone} required />
                  </label>
                  <label className="full">
                    Описание
                    <textarea
                      name="description"
                      defaultValue={current.description ?? ''}
                      rows={2}
                    />
                  </label>
                </div>
                <button className="secondary-button" disabled={busy}>
                  <Save size={15} />
                  Сохранить пространство
                </button>
              </form>
            )}
          </>
        )}
        {panel === 'team' && (
          <>
            <h3>Участники и права</h3>
            <div className="member-list">
              {snapshot.memberships
                .filter((m) => m.workspaceId === currentId)
                .map((member) => {
                  const user = snapshot.users.find((u) => u.id === member.userId);
                  return (
                    <div key={member.userId}>
                      <div className="avatar small">{user?.displayName.charAt(0)}</div>
                      <span>
                        <strong>{user?.displayName}</strong>
                        <small>
                          {user?.email ?? (user?.local ? 'Локальная учётная запись' : '')}
                        </small>
                        {user?.pending && (
                          <small className="invitation-pending">
                            Приглашение · ждёт входа по коду
                          </small>
                        )}
                      </span>
                      <select
                        aria-label={`Роль ${user?.displayName}`}
                        value={member.role}
                        disabled={!owner || member.userId === snapshot.user.id}
                        onChange={(e) =>
                          void act('/api/memberships', 'PATCH', {
                            workspaceId: currentId,
                            userId: member.userId,
                            role: e.target.value,
                          })
                        }
                      >
                        {Object.entries(roleLabels).map(([key, label]) => (
                          <option key={key} value={key}>
                            {label}
                          </option>
                        ))}
                      </select>
                      {owner && user?.pending && (
                        <button
                          className="small-button"
                          disabled={busy || invitationBusy}
                          onClick={() => void renewInvitation(member.userId)}
                        >
                          Новый код
                        </button>
                      )}
                      {owner && member.userId !== snapshot.user.id && (
                        <button
                          className="icon-button danger"
                          aria-label={`Удалить участника ${user?.displayName}`}
                          onClick={() =>
                            void act(
                              `/api/memberships?workspaceId=${encodeURIComponent(currentId)}&userId=${encodeURIComponent(member.userId)}`,
                              'DELETE',
                            )
                          }
                        >
                          <Trash2 size={16} />
                        </button>
                      )}
                    </div>
                  );
                })}
            </div>
            {owner && (
              <form className="settings-section" onSubmit={createMember}>
                <h3>Добавить участника</h3>
                <div className="form-grid">
                  <label>
                    Имя
                    <input
                      value={memberName}
                      onChange={(e) => setMemberName(e.target.value)}
                      required
                    />
                  </label>
                  <label>
                    Email
                    <input
                      type="email"
                      value={memberEmail}
                      onChange={(e) => setMemberEmail(e.target.value)}
                      required
                    />
                  </label>
                  <label>
                    Роль
                    <select
                      value={memberRole}
                      onChange={(e) => setMemberRole(e.target.value as Role)}
                    >
                      {Object.entries(roleLabels).map(([key, label]) => (
                        <option key={key} value={key}>
                          {label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <button className="primary-button form-bottom" disabled={busy}>
                    <Plus size={15} />
                    Добавить
                  </button>
                </div>
                <p className="helper">
                  Передайте участнику одноразовый код. Для входа нужен указанный email; адрес пока
                  не подтверждается письмом. Письма с приглашениями не отправляются.
                </p>
              </form>
            )}
            {owner && invitation && invitation.workspaceId === currentId && (
              <div className="invitation-card" role="status">
                <h3>Код приглашения для {invitation.email}</h3>
                <p className="helper">
                  Код доступен до {dateLabel(invitation.expiresAt, current?.timezone, true)} и
                  исчезнет после закрытия настроек. Скопируйте и передайте его участнику.
                </p>
                <label>
                  Одноразовый код
                  <div className="input-action">
                    <input
                      type={invitationVisible ? 'text' : 'password'}
                      readOnly
                      value={invitation.token}
                      autoComplete="off"
                      spellCheck={false}
                      aria-label="Код приглашения"
                    />
                    <button
                      type="button"
                      className="icon-button"
                      aria-label={invitationVisible ? 'Скрыть код' : 'Показать код'}
                      onClick={() => setInvitationVisible((value) => !value)}
                    >
                      {invitationVisible ? <EyeOff size={17} /> : <Eye size={17} />}
                    </button>
                  </div>
                </label>
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => void copyInvitation()}
                >
                  <Copy size={15} />
                  {invitationCopied ? 'Код скопирован' : 'Скопировать код'}
                </button>
              </div>
            )}
            <p className="helper">
              Редактор меняет объекты. Согласующий принимает или отклоняет предложения. Читатель
              просматривает шкалу. Владелец управляет пространством.
            </p>
          </>
        )}
        {panel === 'types' && (
          <>
            <div className="type-list">
              {snapshot.types
                .filter((t) => !t.workspaceId || t.workspaceId === currentId)
                .map((t) => (
                  <div key={t.id}>
                    <span className="color-dot" style={{ background: t.color }} />
                    <div>
                      <strong>{t.label}</strong>
                      <small>
                        {kindLabels[t.kind]} · {t.fields.length} полей
                        {t.builtin ? ' · встроенный' : ''}
                      </small>
                    </div>
                    {editable && !t.builtin && (
                      <button
                        className="icon-button danger"
                        aria-label={`Удалить тип ${t.label}`}
                        onClick={() => void act(`/api/types/${t.id}`, 'DELETE')}
                      >
                        <Trash2 size={15} />
                      </button>
                    )}
                  </div>
                ))}
            </div>
            {editable && (
              <form className="settings-section" onSubmit={createType}>
                <h3>Свой тип объекта</h3>
                <div className="form-grid">
                  <label>
                    Название
                    <input
                      value={typeName}
                      onChange={(e) => setTypeName(e.target.value)}
                      placeholder="Бронь, ремонт, сон, поставка…"
                      required
                    />
                  </label>
                  <label>
                    Форма
                    <select
                      value={typeKind}
                      onChange={(e) => setTypeKind(e.target.value as EntityKind)}
                    >
                      {Object.entries(kindLabels).map(([key, label]) => (
                        <option key={key} value={key}>
                          {label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Цвет
                    <input
                      type="color"
                      value={typeColor}
                      onChange={(e) => setTypeColor(e.target.value)}
                    />
                  </label>
                </div>
                <h4>Дополнительные поля</h4>
                {fields.map((field) => (
                  <div className="field-definition" key={field.id}>
                    <input
                      aria-label="Название поля"
                      value={field.label}
                      onChange={(e) =>
                        setFields(
                          fields.map((f) =>
                            f.id === field.id ? { ...f, label: e.target.value } : f,
                          ),
                        )
                      }
                      required
                      placeholder="Название поля"
                    />
                    <select
                      aria-label="Тип поля"
                      value={field.type}
                      onChange={(e) =>
                        setFields(
                          fields.map((f) =>
                            f.id === field.id
                              ? { ...f, type: e.target.value as FieldDefinition['type'] }
                              : f,
                          ),
                        )
                      }
                    >
                      <option value="text">Текст</option>
                      <option value="number">Число</option>
                      <option value="boolean">Да / нет</option>
                      <option value="date">Дата</option>
                      <option value="url">Ссылка</option>
                      <option value="select">Выбор</option>
                    </select>
                    <label className="checkbox-label">
                      <input
                        type="checkbox"
                        checked={field.required ?? false}
                        onChange={(e) =>
                          setFields(
                            fields.map((f) =>
                              f.id === field.id ? { ...f, required: e.target.checked } : f,
                            ),
                          )
                        }
                      />
                      Обязательно
                    </label>
                    {field.type === 'select' && (
                      <input
                        aria-label="Варианты выбора"
                        value={field.options?.join(', ') ?? ''}
                        onChange={(e) =>
                          setFields(
                            fields.map((f) =>
                              f.id === field.id
                                ? {
                                    ...f,
                                    options: e.target.value
                                      .split(',')
                                      .map((s) => s.trim())
                                      .filter(Boolean),
                                  }
                                : f,
                            ),
                          )
                        }
                        placeholder="Варианты через запятую"
                      />
                    )}
                    <button
                      type="button"
                      className="icon-button"
                      aria-label="Удалить поле"
                      onClick={() => setFields(fields.filter((f) => f.id !== field.id))}
                    >
                      <X size={15} />
                    </button>
                  </div>
                ))}
                <div className="button-row">
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => setFields([...fields, { id: uid(), label: '', type: 'text' }])}
                  >
                    <Plus size={14} />
                    Поле
                  </button>
                  <button className="primary-button" disabled={busy}>
                    Создать тип
                  </button>
                </div>
              </form>
            )}
          </>
        )}
        {panel === 'resources' && (
          <>
            <div className="type-list">
              {snapshot.resources
                .filter((r) => r.workspaceId === currentId)
                .map((r) => (
                  <div key={r.id}>
                    <Layers3 size={17} />
                    <div>
                      <strong>{r.name}</strong>
                      <small>
                        Доступно {r.capacity} {r.unit} ·{' '}
                        {r.workingWeekdays
                          .map((n) => ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'][n - 1])
                          .join(', ')}
                      </small>
                    </div>
                    {editable && (
                      <button
                        className="icon-button danger"
                        onClick={() => void act(`/api/resources/${r.id}`, 'DELETE')}
                        aria-label={`Удалить ресурс ${r.name}`}
                      >
                        <Trash2 size={15} />
                      </button>
                    )}
                  </div>
                ))}
            </div>
            {editable && (
              <form className="settings-section" onSubmit={createResource}>
                <h3>Добавить ресурс</h3>
                <div className="form-grid">
                  <label>
                    Название
                    <input
                      value={resourceName}
                      onChange={(e) => setResourceName(e.target.value)}
                      required
                      placeholder="Команда, автомобиль, бюджет…"
                    />
                  </label>
                  <label>
                    Категория
                    <select value={resourceKind} onChange={(e) => setResourceKind(e.target.value)}>
                      <option value="person">Люди</option>
                      <option value="equipment">Оборудование</option>
                      <option value="place">Место</option>
                      <option value="budget">Бюджет</option>
                      <option value="other">Другое</option>
                    </select>
                  </label>
                  <label>
                    Доступное количество
                    <input
                      type="number"
                      step="any"
                      min={0.01}
                      value={capacity}
                      onChange={(e) => setCapacity(Number(e.target.value))}
                      required
                    />
                  </label>
                  <label>
                    Единица
                    <input value={unit} onChange={(e) => setUnit(e.target.value)} required />
                  </label>
                  <div className="weekday-picker full">
                    {['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'].map((day, i) => (
                      <label key={day}>
                        <input
                          type="checkbox"
                          checked={weekdays.includes(i + 1)}
                          onChange={(e) =>
                            setWeekdays(
                              e.target.checked
                                ? [...weekdays, i + 1]
                                : weekdays.filter((n) => n !== i + 1),
                            )
                          }
                        />
                        {day}
                      </label>
                    ))}
                  </div>
                </div>
                <button className="primary-button" disabled={busy}>
                  <Plus size={15} />
                  Создать ресурс
                </button>
              </form>
            )}
          </>
        )}
        {panel === 'rules' && (
          <>
            <p className="helper">
              Правила выполняются сервером. Повторные срабатывания объединяются, чтобы не засорять
              шкалу.
            </p>
            <div className="type-list">
              {snapshot.rules
                .filter((r) => r.workspaceId === currentId)
                .map((rule) => (
                  <div key={rule.id}>
                    <Workflow size={17} />
                    <div>
                      <strong>{rule.name}</strong>
                      <small>
                        {rule.action === 'signal' ? 'Оповещение' : 'Следующий объект'} ·{' '}
                        {rule.leadMinutes} мин.
                      </small>
                    </div>
                    <label className="checkbox-label">
                      <input
                        type="checkbox"
                        disabled={!editable}
                        checked={rule.enabled}
                        onChange={(e) =>
                          void act(`/api/rules/${rule.id}`, 'PATCH', { enabled: e.target.checked })
                        }
                      />
                      Включено
                    </label>
                    {editable && (
                      <button
                        className="icon-button danger"
                        aria-label="Удалить правило"
                        onClick={() => void act(`/api/rules/${rule.id}`, 'DELETE')}
                      >
                        <Trash2 size={15} />
                      </button>
                    )}
                  </div>
                ))}
            </div>
            {editable && (
              <form className="settings-section" onSubmit={createRule}>
                <h3>Новое правило</h3>
                <div className="form-grid">
                  <label>
                    Название
                    <input
                      value={ruleName}
                      onChange={(e) => setRuleName(e.target.value)}
                      required
                    />
                  </label>
                  <label>
                    Когда
                    <select value={trigger} onChange={(e) => setTrigger(e.target.value)}>
                      <option value="before-start">До начала</option>
                      <option value="before-due">До крайнего срока</option>
                      <option value="after-done">После завершения</option>
                      <option value="overdue">При просрочке</option>
                      <option value="resource-conflict">При конфликте ресурсов</option>
                    </select>
                  </label>
                  <label>
                    Интервал, минуты
                    <input
                      type="number"
                      min={0}
                      value={lead}
                      onChange={(e) => setLead(Number(e.target.value))}
                    />
                  </label>
                  <label>
                    Объекты
                    <select value={ruleType} onChange={(e) => setRuleType(e.target.value)}>
                      <option value="">Все типы</option>
                      {snapshot.types
                        .filter((t) => !t.workspaceId || t.workspaceId === currentId)
                        .map((t) => (
                          <option key={t.id} value={t.id}>
                            {t.label}
                          </option>
                        ))}
                    </select>
                  </label>
                  <label>
                    Действие
                    <select value={ruleAction} onChange={(e) => setRuleAction(e.target.value)}>
                      <option value="signal">Создать оповещение</option>
                      <option value="create-followup">Создать следующий объект</option>
                    </select>
                  </label>
                  {ruleAction === 'create-followup' && (
                    <label>
                      Название следующего объекта
                      <input
                        value={followup}
                        onChange={(e) => setFollowup(e.target.value)}
                        required
                      />
                    </label>
                  )}
                </div>
                <button className="primary-button" disabled={busy}>
                  <Plus size={15} />
                  Добавить правило
                </button>
              </form>
            )}
          </>
        )}
        {panel === 'notifications' && (
          <form onSubmit={saveNotifications}>
            <h3>Оповещения с учётом вашего времени</h3>
            <div className="form-grid">
              <label>
                Тихие часы: с
                <input
                  type="time"
                  value={notificationPreferences.quietStart}
                  onChange={(e) =>
                    setNotificationPreferences((p) => ({ ...p, quietStart: e.target.value }))
                  }
                />
              </label>
              <label>
                До
                <input
                  type="time"
                  value={notificationPreferences.quietEnd}
                  onChange={(e) =>
                    setNotificationPreferences((p) => ({ ...p, quietEnd: e.target.value }))
                  }
                />
              </label>
              <label>
                Часовой пояс
                <input
                  value={notificationPreferences.timezone}
                  onChange={(e) =>
                    setNotificationPreferences((p) => ({ ...p, timezone: e.target.value }))
                  }
                  required
                />
              </label>
              <label>
                Повторять через (мин.)
                <input
                  type="number"
                  min={1}
                  value={notificationPreferences.repeatMinutes}
                  onChange={(e) =>
                    setNotificationPreferences((p) => ({
                      ...p,
                      repeatMinutes: Number(e.target.value),
                    }))
                  }
                />
              </label>
              <label>
                Повысить важность через (мин.)
                <input
                  type="number"
                  min={1}
                  value={notificationPreferences.escalationMinutes}
                  onChange={(e) =>
                    setNotificationPreferences((p) => ({
                      ...p,
                      escalationMinutes: Number(e.target.value),
                    }))
                  }
                />
              </label>
              <label className="checkbox-label full">
                <input
                  type="checkbox"
                  checked={notificationPreferences.browserEnabled}
                  onChange={(e) =>
                    setNotificationPreferences((p) => ({ ...p, browserEnabled: e.target.checked }))
                  }
                />
                Включить оповещения браузера
              </label>
            </div>
            <p className="helper">
              Разрешение запрашивается при сохранении. Оповещения внутри шкалы работают независимо
              от браузерного разрешения.
            </p>
            <button className="primary-button" disabled={busy}>
              <Save size={15} />
              Сохранить
            </button>
          </form>
        )}
        {panel === 'data' && (
          <>
            <h3>Перенести данные</h3>
            <div className="form-grid">
              <label>
                Формат
                <select
                  value={format}
                  onChange={(e) => setFormat(e.target.value as 'json' | 'csv' | 'ics')}
                >
                  <option value="json">JSON — структура и записи</option>
                  <option value="csv">CSV — таблица объектов</option>
                  <option value="ics">ICS — календарь</option>
                </select>
              </label>
              <button
                type="button"
                className="secondary-button form-bottom"
                disabled={exporting || busy || !currentId}
                onClick={() => void exportData()}
              >
                <Download size={15} />
                {exporting ? 'Подготовка…' : 'Экспортировать'}
              </button>
            </div>
            <p className="helper">
              ICS переносит календарное время. Для всей структуры и повторений с переносом
              невозможных дат используйте JSON. Для резервной копии вложений сохраните также каталог
              data сервера.
            </p>
            {editable && (
              <form className="settings-section" onSubmit={importData}>
                <h3>Импортировать</h3>
                <label className="secondary-button upload-button">
                  <FileUp size={15} />
                  {importName || 'Выбрать файл'}
                  <input
                    type="file"
                    accept=".json,.csv,.ics,.txt"
                    onChange={async (e) => {
                      const file = e.target.files?.[0];
                      if (file) {
                        setImportName(file.name);
                        setImportText(await file.text());
                        const extension = file.name.split('.').at(-1);
                        if (['json', 'csv', 'ics'].includes(extension ?? ''))
                          setFormat(extension as 'json' | 'csv' | 'ics');
                      }
                    }}
                  />
                </label>
                <label>
                  Содержимое
                  <textarea
                    value={importText}
                    onChange={(e) => setImportText(e.target.value)}
                    rows={7}
                    placeholder={
                      format === 'csv'
                        ? 'title,start,end,typeId\nОтпуск,2026-11-01,2026-11-10,period'
                        : format === 'ics'
                          ? 'BEGIN:VCALENDAR…'
                          : 'JSON из экспорта планировщика или массив объектов'
                    }
                  />
                </label>
                <p className="helper">
                  Импорт проверяется целиком. При ошибке ничего не сохраняется. JSON переносит
                  структуру и записи; содержимое загруженных файлов хранится на сервере отдельно.
                </p>
                <button className="primary-button" disabled={busy || !importText.trim()}>
                  <FileUp size={15} />
                  Импортировать в «{current?.name}»
                </button>
              </form>
            )}
            {editable && (
              <form className="settings-section" onSubmit={saveTemplate}>
                <h3>Сохранить процесс как шаблон</h3>
                <div className="form-grid">
                  <label>
                    Процесс
                    <select
                      value={templateSource}
                      onChange={(e) => setTemplateSource(e.target.value)}
                      required
                    >
                      <option value="">Выбрать процесс</option>
                      {snapshot.entities
                        .filter((e) => e.workspaceId === currentId && e.kind === 'process')
                        .map((e) => (
                          <option key={e.id} value={e.id}>
                            {e.title}
                          </option>
                        ))}
                    </select>
                  </label>
                  <label>
                    Название шаблона
                    <input
                      value={templateName}
                      onChange={(e) => setTemplateName(e.target.value)}
                      required
                      placeholder="Моя поездка, запуск, отпуск…"
                    />
                  </label>
                </div>
                <p className="helper">
                  Сохраняются этапы, поля и связи. При использовании даты сдвигаются к новому
                  началу.
                </p>
                <button className="secondary-button" disabled={busy || !templateSource}>
                  <Save size={15} />
                  Сохранить шаблон
                </button>
              </form>
            )}
          </>
        )}
        {panel === 'account' && (
          <>
            <div className="account-card">
              <span className="avatar">{snapshot.user.displayName.charAt(0)}</span>
              <div>
                <h3>{snapshot.user.displayName}</h3>
                <p>
                  {snapshot.user.local ? 'Локальный режим на этом устройстве' : snapshot.user.email}
                </p>
              </div>
            </div>
            {snapshot.user.local && (
              <p className="helper">
                Сейчас данные доступны локальной учётной записи. При создании аккаунта они
                привязываются к вашему email.
              </p>
            )}
            {snapshot.user.local ? (
              <>
                <div className="segmented-control">
                  <button
                    className={authMode === 'register' ? 'active' : ''}
                    onClick={() => setAuthMode('register')}
                  >
                    Создать аккаунт
                  </button>
                  <button
                    className={authMode === 'login' ? 'active' : ''}
                    onClick={() => setAuthMode('login')}
                  >
                    Войти
                  </button>
                </div>
                <form onSubmit={authenticate}>
                  <div className="form-grid">
                    {authMode === 'register' && (
                      <label className="full">
                        Имя
                        <input
                          value={displayName}
                          onChange={(e) => setDisplayName(e.target.value)}
                          required
                        />
                      </label>
                    )}
                    <label>
                      Email
                      <input
                        type="email"
                        autoComplete="email"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        required
                      />
                    </label>
                    <label>
                      Пароль
                      <input
                        type="password"
                        autoComplete={authMode === 'register' ? 'new-password' : 'current-password'}
                        minLength={10}
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                        required
                      />
                      <small>Минимум 10 символов</small>
                    </label>
                    {authMode === 'register' && (
                      <label className="full">
                        Код приглашения <span className="field-optional">если вас пригласили</span>
                        <input
                          type="password"
                          value={invitationToken}
                          onChange={(e) => setInvitationToken(e.target.value)}
                          autoComplete="off"
                          spellCheck={false}
                        />
                      </label>
                    )}
                  </div>
                  <button className="primary-button" disabled={busy}>
                    {authMode === 'register' ? 'Создать аккаунт и сохранить данные' : 'Войти'}
                  </button>
                </form>
              </>
            ) : (
              <>
                <form className="settings-section" onSubmit={acceptInvitation}>
                  <h3>Присоединиться к пространству</h3>
                  <label>
                    Код приглашения
                    <input
                      type="password"
                      value={invitationToken}
                      onChange={(e) => setInvitationToken(e.target.value)}
                      autoComplete="off"
                      spellCheck={false}
                      required
                    />
                  </label>
                  <p className="helper">
                    Код даёт доступ к одному пространству. Email приглашения должен совпадать с
                    вашим аккаунтом.
                  </p>
                  <button className="primary-button" disabled={busy || !invitationToken.trim()}>
                    Принять приглашение
                  </button>
                </form>
                <button
                  className="secondary-button"
                  onClick={async () => {
                    try {
                      await request('/api/auth/logout', 'POST');
                      await refresh();
                    } catch (e) {
                      setError((e as Error).message);
                    }
                  }}
                >
                  Выйти
                </button>
              </>
            )}
          </>
        )}
        {error && (
          <p className="inline-error" role="alert">
            {error}
          </p>
        )}
        {success && (
          <p className="inline-success" role="status">
            {success}
          </p>
        )}
      </div>
      <footer className="modal-footer">
        <span className="helper">Настройки сохраняются на сервере</span>
        <button className="secondary-button" onClick={onClose}>
          Готово
        </button>
      </footer>
    </Modal>
  );
}
