import {
  rasterFile,
  captureSelectedDisplay,
  createFeedbackRecorder,
  attachmentBytes,
  validateAttachmentBudget,
} from "/assets/feedback-media.mjs";

const root = document.querySelector("#app"),
  origin = location.origin;
let csrfToken = null;
const states = {
  received: "Принято",
  in_progress: "В работе",
  needs_action: "Нужно ваше действие",
  ready_to_check: "Готово для проверки",
  resolved: "Решено",
};
const make = (tag, className = "", text = "") => {
  const node = document.createElement(tag);
  node.className = className;
  node.textContent = text;
  return node;
};
const button = (label, action, primary = false) => {
  const node = make("button", primary ? "primary" : "", label);
  node.type = "button";
  node.addEventListener("click", action);
  return node;
};
const link = (label, url, className = "") => {
  const node = make("a", className, label);
  node.href = url;
  return node;
};
const field = (label, input) => {
  const node = make("label", "field");
  node.append(make("span", "", label), input);
  return node;
};
const input = (placeholder = "", type = "text") => {
  const node = make("input");
  node.type = type;
  node.placeholder = placeholder;
  return node;
};
const date = (value) => new Date(value).toLocaleString("ru-RU");
const errors = {
  authentication_required: "Войдите в свой аккаунт, чтобы продолжить.",
  project_unavailable: "Проект недоступен для этого аккаунта.",
  feedback_revision_conflict:
    "Обращение уже изменилось. Обновите его перед следующим действием.",
  request_conflict:
    "Этот запрос уже использован с другим содержанием. Проверьте список проектов.",
  rate_limit: "Слишком много запросов. Попробуйте через минуту.",
  feedback_attachment_bytes: "Вложения вместе должны занимать не больше 1 МиБ.",
  feedback_attachment_count: "Можно приложить до трёх файлов.",
  feedback_capture_unavailable:
    "Снимок экрана здесь недоступен. Можно выбрать изображение из файла.",
  feedback_record_unavailable:
    "Запись голоса здесь недоступна. Можно отправить текст.",
  feedback_invalid_body: "Добавьте текст сообщения, не больше 8000 символов.",
  feedback_audio_duration_limit:
    "Аудиозапись должна быть не длиннее двух минут.",
  feedback_invalid_cursor: "Список изменился. Обновите страницу.",
  read_only: "Для этого подключения разрешено только чтение.",
};
function describe(error) {
  return (
    errors[error.code ?? error.message] ??
    (error.status === 401
      ? "Срок доступа закончился. Войдите заново."
      : error.status === 413
        ? "Сообщение или вложения слишком велики."
        : "Не получили подтверждение. Точный запрос сохранён для повторной проверки.")
  );
}
function notice(node, text, error = false) {
  node.textContent = text;
  node.className = "notice" + (error ? " error" : "");
}
async function request(path, body, token) {
  const response = await fetch(path, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      ...(token
        ? { Authorization: "Bearer " + token }
        : csrfToken && body !== undefined
          ? { "X-Feedback-CSRF": csrfToken }
          : {}),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    credentials: token ? "omit" : "same-origin",
    redirect: "error",
  });
  const value = await response.json();
  if (!response.ok || value.isError) {
    const error = new Error();
    error.status = response.status;
    error.code = value.error?.code;
    if (value.isError)
      try {
        error.code = JSON.parse(value.content[0].text).error.code;
      } catch {}
    throw error;
  }
  return value;
}
let sourceClient = null;
async function ownerCall(operation, args = {}) {
  if (sourceClient) return sourceClient.call(operation, args);
  const result = await request("/api/tools/feedback_" + operation, args);
  if (operation === "attachment_get") {
    const media = result.content.find(
      (item) => item.type === "resource",
    )?.resource;
    return {
      attachment: {
        ...result.structuredContent.attachment,
        dataBase64: media?.blob,
      },
    };
  }
  return result.structuredContent;
}
function hero(title, subtitle) {
  const node = make("section", "hero");
  node.append(
    make("div", "eyebrow", "Соты · Feedback MCP"),
    make("h1", "", title),
    make("p", "lead", subtitle),
  );
  return node;
}
function code(text) {
  const node = make("pre", "code");
  node.append(make("code", "", text));
  return node;
}
async function copy(value, target) {
  try {
    await navigator.clipboard.writeText(value);
    notice(target, "Скопировано.");
  } catch {
    notice(
      target,
      "Копирование недоступно. Выделите и скопируйте текст вручную.",
      true,
    );
  }
}

function guide() {
  root.replaceChildren(
    hero(
      "Подключите через своего агента",
      "Один MCP для проектов на любом стеке. Обращения можно читать и обрабатывать из вашей среды с ИИ.",
    ),
  );
  const card = make("section", "card"),
    url = origin + "/mcp",
    message = make("p", "notice");
  card.append(
    make("h2", "", "Адрес MCP"),
    code(url),
    button("Скопировать адрес", () => copy(url, message)),
    message,
  );
  const steps = make("ol", "guide-list");
  for (const text of [
    "Добавьте удалённый MCP по этому адресу в среду, которая поддерживает HTTP MCP и вход через OAuth.",
    "Войдите через действующий аккаунт Сот и подтвердите доступ. Аккаунт проекта и данные других владельцев остаются отдельными.",
    "Попросите агента: «Подключи обратную связь к этому проекту». Агент найдёт существующий проект или создаст новый.",
    "Агент получит ссылку формы и код кнопки. Для изменения исходников ему нужен доступ к вашему проекту.",
    "Попросите: «Покажи новые обращения по проекту». Ответы людям и исправления выполняются по вашему поручению.",
  ])
    steps.append(make("li", "", text));
  card.append(steps);
  root.append(card);
  const grid = make("div", "grid"),
    left = make("section", "card"),
    right = make("section", "card");
  left.append(
    make("h2", "", "Для пользователей проекта"),
    make(
      "p",
      "",
      "Форма по ссылке или кнопка на сайте: текст, изображение и голос. Регистрация для открытой формы не требуется. Личная ссылка на историю даёт доступ только к своим обращениям.",
    ),
  );
  right.append(
    make("h2", "", "Для владельца"),
    make(
      "p",
      "",
      "Читайте обращения через MCP или в этом кабинете. Передавайте готовое решение на проверку; подтверждает его автор обращения.",
    ),
    link("Открыть кабинет", "/", "button primary"),
  );
  grid.append(left, right);
  root.append(
    grid,
    make(
      "p",
      "muted",
      "При настройке виджета разрешите этот домен в CSP вашего сайта. Если встраивание недоступно, используйте обычную ссылку. Голос сохраняется как аудио без автоматической расшифровки.",
    ),
  );
}

let pendingWrites = new Map();
window.addEventListener("beforeunload", (event) => {
  if (pendingWrites.size) {
    event.preventDefault();
    event.returnValue = "";
  }
});
function writeOnce(call, operation, args) {
  const key = operation + ":" + (args.ticketId ?? args.projectId ?? "new");
  const intent = pendingWrites.get(key) ?? {
    ...args,
    requestId: crypto.randomUUID(),
  };
  pendingWrites.set(key, intent);
  return call(operation, intent)
    .then((result) => {
      pendingWrites.delete(key);
      return result;
    })
    .catch((error) => {
      if ([400, 401, 403, 404, 409, 413, 415].includes(error.status))
        pendingWrites.delete(key);
      throw error;
    });
}
function attachmentView(items, onLoad) {
  const box = make("div", "media");
  for (const item of items ?? []) {
    const card = make("div");
    if (item.dataBase64) {
      const source = "data:" + item.mimeType + ";base64," + item.dataBase64;
      if (item.kind === "image") {
        const image = make("img");
        image.src = source;
        image.alt = item.name;
        card.append(image);
      } else {
        const audio = make("audio");
        audio.controls = true;
        audio.preload = "none";
        audio.src = source;
        card.append(audio);
      }
    } else {
      card.append(make("p", "muted", item.name));
      if (onLoad)
        card.append(
          button("Открыть вложение", async (event) => {
            event.currentTarget.disabled = true;
            await onLoad(item);
          }),
        );
    }
    box.append(card);
  }
  return box;
}

function inbox(host, projectId, call, { owner = false, token } = {}) {
  let selected = null,
    sequence = 0,
    cursor = null,
    conversation = null;
  const uncertain = new Map(),
    replyDrafts = new Map(),
    list = make("div"),
    detail = make("div"),
    message = make("p", "notice");
  const refresh = button("Обновить", async () => {
      await load();
      if (selected) await getTicket(selected.id);
    }),
    heading = make("div", "row spread");
  heading.append(
    make("h2", "", owner ? "Обращения проекта" : "Ваши обращения"),
    refresh,
  );
  host.replaceChildren(heading, message, list, detail);
  async function getTicket(ticketId, messageOffset = 0) {
    const stamp = ++sequence;
    try {
      const value = await call("ticket_get", {
        projectId,
        ticketId,
        ...(owner ? { messageOffset, messageLimit: 20 } : {}),
      });
      if (stamp !== sequence || !host.isConnected) return;
      selected = value.ticket;
      conversation = value.messagesPage ?? null;
      render();
    } catch (error) {
      notice(message, describe(error), true);
    }
  }
  async function mutate(operation, args) {
    const ticketId = selected.id,
      stamp = sequence;
    const loadedMedia = new Map(
      (selected.attachments ?? []).map((item) => [item.id, item]),
    );
    const controls = [...detail.querySelectorAll("button,textarea")];
    controls.forEach((node) => (node.disabled = true));
    try {
      const value = await writeOnce(call, operation, {
        projectId,
        ticketId,
        expectedRevision: selected.revision,
        ...args,
      });
      if (operation === "ticket_reply") replyDrafts.delete(ticketId);
      uncertain.delete(ticketId);
      if (!host.isConnected) return;
      if (selected?.id === ticketId && sequence === stamp) {
        selected = value.ticket;
        selected.attachments = selected.attachments.map((item) => {
          const previous = loadedMedia.get(item.id);
          return previous?.dataBase64 &&
            previous.mimeType === item.mimeType &&
            previous.byteLength === item.byteLength
            ? { ...item, dataBase64: previous.dataBase64 }
            : item;
        });
        conversation = value.messagesPage ?? null;
        notice(message, "Сохранено.");
        render();
      }
      await load();
    } catch (error) {
      if (!error.status || error.status >= 500 || error.status === 429)
        uncertain.set(ticketId, { operation, args });
      else uncertain.delete(ticketId);
      if (host.isConnected && selected?.id === ticketId) {
        notice(message, describe(error), true);
        render();
      }
    }
  }
  function render() {
    detail.replaceChildren();
    if (!selected) return;
    const card = make("section", "card"),
      heading = make("div", "row spread");
    heading.append(
      make("h3", "", "Обращение"),
      make("span", "badge", states[selected.status] ?? selected.status),
    );
    const selectedId = selected.id;
    card.append(
      heading,
      make("small", "", date(selected.createdAt)),
      make("p", "body", selected.body),
      attachmentView(
        selected.attachments,
        owner
          ? async (item) => {
              try {
                const value = await call("attachment_get", {
                  projectId,
                  ticketId: selectedId,
                  attachmentId: item.id,
                });
                if (selected?.id !== selectedId || !host.isConnected) return;
                selected.attachments = selected.attachments.map((attachment) =>
                  attachment.id === item.id ? value.attachment : attachment,
                );
                render();
              } catch (error) {
                notice(message, describe(error), true);
                render();
              }
            }
          : undefined,
      ),
    );
    for (const item of selected.messages ?? []) {
      const node = make(
        "div",
        "message " + (item.kind === "support" ? "support" : ""),
      );
      node.append(
        make(
          "strong",
          "",
          item.kind === "support" ? "Владелец проекта" : "Автор обращения",
        ),
        make("p", "body", item.body),
        make("small", "", date(item.createdAt)),
      );
      card.append(node);
    }
    if (conversation && conversation.total > selected.messages.length) {
      const navigation = make("div", "row");
      navigation.append(
        make(
          "small",
          "",
          `Ответы ${conversation.offset + 1}–${conversation.offset + selected.messages.length} из ${conversation.total}`,
        ),
      );
      if (conversation.offset > 0)
        navigation.append(button("С начала", () => getTicket(selected.id, 0)));
      if (conversation.nextOffset !== null)
        navigation.append(
          button("Следующие ответы", () =>
            getTicket(selected.id, conversation.nextOffset),
          ),
        );
      navigation.append(
        button("Последний ответ", () =>
          getTicket(selected.id, conversation.total - 1),
        ),
      );
      card.append(navigation);
    }
    const actions = make("div", "row");
    if (owner && selected.canManage && selected.status !== "resolved") {
      actions.append(
        button("В работу", () =>
          mutate("ticket_status", { status: "in_progress" }),
        ),
        button("Нужно уточнение", () =>
          mutate("ticket_status", { status: "needs_action" }),
        ),
        button("Передать на проверку", () =>
          mutate("ticket_status", { status: "ready_to_check" }),
        ),
      );
    }
    if (selected.canAccept && selected.status === "ready_to_check")
      actions.append(
        button("Проблема решена", () => mutate("ticket_accept", {}), true),
      );
    card.append(actions);
    if (selected.canReply) {
      const reply = make("textarea");
      reply.maxLength = 8000;
      reply.rows = 3;
      reply.value = replyDrafts.get(selected.id) ?? "";
      reply.addEventListener("input", () =>
        replyDrafts.set(selected.id, reply.value),
      );
      const form = make("form");
      const send = make("button", "primary", "Отправить ответ");
      send.type = "submit";
      form.append(field(owner ? "Ответ владельца" : "Ваш ответ", reply), send);
      form.addEventListener("submit", (event) => {
        event.preventDefault();
        if (reply.value.trim())
          void mutate("ticket_reply", { body: reply.value });
      });
      card.append(form);
    }
    if (uncertain.has(selected.id)) {
      const held = uncertain.get(selected.id);
      card
        .querySelectorAll("button,textarea")
        .forEach((node) => (node.disabled = true));
      card.append(
        make(
          "p",
          "notice",
          "Предыдущее изменение ожидает подтверждения. Проверка использует тот же запрос и содержание. До подтверждения не закрывайте страницу.",
        ),
        button(
          "Проверить сохранение",
          () => mutate(held.operation, held.args),
          true,
        ),
      );
    }
    detail.append(card);
  }
  async function load(reset = true) {
    try {
      const value = await call("tickets_list", {
        projectId,
        limit: 20,
        ...(!reset && cursor ? { cursor } : {}),
      });
      if (!host.isConnected) return;
      if (reset) list.replaceChildren();
      cursor = value.nextCursor;
      if (!value.tickets.length && !list.childElementCount)
        list.append(make("p", "empty", "Здесь появятся обращения."));
      for (const ticket of value.tickets) {
        const item = button("", () => getTicket(ticket.id));
        item.className = "ticket";
        const text = make("span");
        text.append(
          make("strong", "", ticket.body),
          make("small", "", date(ticket.createdAt)),
        );
        item.append(
          text,
          make("span", "badge", states[ticket.status] ?? ticket.status),
        );
        list.append(item);
      }
      if (cursor)
        list.append(
          button("Показать ещё", (event) => {
            event.currentTarget.remove();
            void load(false);
          }),
        );
    } catch (error) {
      notice(message, describe(error), true);
    }
  }
  void load();
  return { select: getTicket };
}

async function publicForm(projectId) {
  const data = await request("/api/public/" + projectId),
    project = data.project;
  const key = "feedback:" + projectId,
    storage = {
      get(name) {
        try {
          return sessionStorage.getItem(key + ":" + name);
        } catch {
          return null;
        }
      },
      set(name, value) {
        try {
          value === null
            ? sessionStorage.removeItem(key + ":" + name)
            : sessionStorage.setItem(key + ":" + name, value);
          return true;
        } catch {
          return false;
        }
      },
    };
  let token = storage.get("access"),
    pending = null,
    attachments = [],
    recording = null,
    busy = false;
  const fragment = new URLSearchParams(location.hash.slice(1)).get("access");
  if (fragment) {
    if (/^fbg_[A-Za-z0-9_-]{43}$/.test(fragment)) {
      token = fragment;
      storage.set("access", token);
    }
    history.replaceState(null, "", location.pathname);
  }
  try {
    const saved = storage.get("pending");
    if (saved && saved.length < 1500000) {
      pending = JSON.parse(saved);
      validateAttachmentBudget(pending.attachments);
      attachments = pending.attachments;
    }
  } catch {
    pending = null;
    storage.set("pending", null);
  }
  root.classList.add("form");
  root.replaceChildren(
    hero(
      project.name,
      "Расскажите, что произошло или что стоит улучшить. Сообщение получит владелец проекта.",
    ),
  );
  const tabs = make("div", "tabs"),
    compose = make("section", "card"),
    historyHost = make("section", "card"),
    message = make("p", "notice");
  historyHost.hidden = true;
  const writeTab = button("Написать", () => {
    compose.hidden = false;
    historyHost.hidden = true;
    writeTab.setAttribute("aria-pressed", "true");
    historyTab.setAttribute("aria-pressed", "false");
  });
  writeTab.setAttribute("aria-pressed", "true");
  const historyTab = button("История", () => {
    compose.hidden = true;
    historyHost.hidden = false;
    writeTab.setAttribute("aria-pressed", "false");
    historyTab.setAttribute("aria-pressed", "true");
    showHistory();
  });
  historyTab.setAttribute("aria-pressed", "false");
  tabs.append(writeTab, historyTab);
  const form = make("form"),
    body = make("textarea");
  body.maxLength = 8000;
  body.rows = 6;
  body.required = true;
  body.placeholder = "Что произошло? Какого результата вы ожидали?";
  body.value = pending?.body ?? "";
  const media = make("div", "row"),
    previews = make("div", "media"),
    file = make("input");
  file.type = "file";
  file.accept = "image/png,image/jpeg,image/webp";
  file.hidden = true;
  const fileButton = button("Выбрать снимок", () => file.click()),
    captureButton = button("Снимок экрана", () =>
      addMedia(() => captureSelectedDisplay(remaining())),
    );
  const voiceButton = button("Записать голос", () => {
    if (recording) {
      recording.stop();
      return;
    }
    if (busy || pending) return;
    const controller = new AbortController();
    recording = createFeedbackRecorder({
      maxBytes: remaining(),
      signal: controller.signal,
      onTick: (seconds) =>
        (voiceButton.textContent = `Остановить · ${seconds} с`),
    });
    controls();
    recording
      .start()
      .then((item) => {
        attachments.push(item);
        renderMedia();
      })
      .catch((error) => notice(message, describe(error), true))
      .finally(() => {
        recording = null;
        voiceButton.textContent = "Записать голос";
        controls();
      });
  });
  file.addEventListener("change", () => {
    const chosen = file.files[0];
    if (chosen) void addMedia(() => rasterFile(chosen, remaining()));
    file.value = "";
  });
  media.append(fileButton, captureButton, voiceButton, file);
  const submit = make(
    "button",
    "primary full",
    pending ? "Проверить отправку" : "Отправить",
  );
  submit.type = "submit";
  form.append(
    field("Сообщение", body),
    media,
    make(
      "p",
      "muted",
      "До трёх вложений, вместе до 1 МиБ. Голос — до двух минут, без автоматической расшифровки.",
    ),
    previews,
    submit,
  );
  compose.append(message, form);
  root.append(
    tabs,
    compose,
    historyHost,
    make(
      "p",
      "muted",
      "История сохраняется для этой вкладки. Чтобы вернуться с другого устройства, сохраните личную ссылку после отправки. Не передавайте её другим людям.",
    ),
  );
  function remaining() {
    return (
      1048576 -
      attachments.reduce((sum, item) => sum + attachmentBytes(item), 0)
    );
  }
  function controls() {
    body.disabled = busy || !!pending;
    submit.disabled = busy || !!recording;
    submit.textContent = pending ? "Проверить отправку" : "Отправить";
    fileButton.disabled = captureButton.disabled =
      busy || !!pending || !!recording || attachments.length >= 3;
    voiceButton.disabled =
      busy || !!pending || (!recording && attachments.length >= 3);
  }
  function renderMedia() {
    previews.replaceChildren();
    attachments.forEach((item, index) => {
      const view = attachmentView([item]).firstChild;
      if (!pending)
        view.append(
          button("Удалить", () => {
            attachments.splice(index, 1);
            renderMedia();
            controls();
          }),
        );
      previews.append(view);
    });
    controls();
  }
  async function addMedia(get) {
    if (busy || pending || attachments.length >= 3) return;
    busy = true;
    controls();
    try {
      const item = await get();
      attachments.push(item);
      validateAttachmentBudget(attachments);
      renderMedia();
    } catch (error) {
      notice(message, describe(error), true);
    } finally {
      busy = false;
      controls();
    }
  }
  async function ensureGuest() {
    if (token) return;
    const value = await request("/api/public/" + projectId + "/session", {});
    token = value.token;
    storage.set("access", token);
  }
  const call = async (operation, args) => {
    await ensureGuest();
    const { projectId: _, ...payload } = args;
    return request(
      "/api/public/" + projectId + "/tools/" + operation,
      payload,
      token,
    );
  };
  function showHistory(ticketId) {
    if (!token) {
      historyHost.replaceChildren(
        make(
          "p",
          "empty",
          "Отправьте сообщение, чтобы появилась ваша история.",
        ),
      );
      return;
    }
    const box = make("div"),
      actions = make("div", "row"),
      info = make("p", "notice");
    actions.append(
      button("Скопировать личную ссылку", () =>
        copy(
          origin + "/p/" + projectId + "#access=" + encodeURIComponent(token),
          info,
        ),
      ),
    );
    historyHost.replaceChildren(actions, info, box);
    const view = inbox(box, projectId, call, { token });
    if (ticketId) void view.select(ticketId);
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (busy || recording) return;
    if (!pending) {
      pending = {
        body: body.value,
        attachments: structuredClone(attachments),
        requestId: crypto.randomUUID(),
      };
      if (!storage.set("pending", JSON.stringify(pending)))
        notice(
          message,
          "Хранилище вкладки недоступно. До подтверждения отправки не закрывайте страницу.",
        );
    }
    busy = true;
    controls();
    try {
      const value = await call("ticket_submit", { projectId, ...pending });
      pending = null;
      storage.set("pending", null);
      body.value = "";
      attachments = [];
      renderMedia();
      notice(message, "Обращение принято. Ответ появится в истории.");
      compose.hidden = true;
      historyHost.hidden = false;
      writeTab.setAttribute("aria-pressed", "false");
      historyTab.setAttribute("aria-pressed", "true");
      showHistory(value.ticket.id);
    } catch (error) {
      if ([400, 409, 413, 415].includes(error.status)) {
        pending = null;
        storage.set("pending", null);
      }
      notice(message, describe(error), true);
    } finally {
      busy = false;
      controls();
    }
  });
  renderMedia();
  window.addEventListener("pagehide", () => recording?.cancel(), {
    once: true,
  });
}

async function dashboard() {
  let session;
  if (location.pathname === "/embed") {
    const { createFeedbackSourceClient } = await import(
      "/assets/source-client.js"
    );
    sourceClient = await createFeedbackSourceClient();
    session = { authenticated: true };
  } else {
    session = await request("/api/me");
    csrfToken = session.csrfToken ?? null;
  }
  root.replaceChildren(
    hero(
      "Обратная связь для любого проекта",
      "Подключите сайт, приложение или просто поделитесь ссылкой. Читайте обращения здесь и через своего ИИ-агента.",
    ),
  );
  if (!session.authenticated) {
    const card = make("section", "card");
    card.append(
      make("h2", "", "Начните со своего аккаунта"),
      make(
        "p",
        "",
        "Войдите, чтобы создать проект или открыть свои обращения. Для отправки через открытую форму вход не требуется.",
      ),
      make("div", "row"),
    );
    card.lastChild.append(
      link("Войти через Соты", "/auth/login", "button primary"),
      link("Подключить MCP", "/guide", "button"),
    );
    root.append(card);
    return;
  }
  const toolbar = make("div", "row spread"),
    message = make("p", "notice"),
    grid = make("div", "grid workspace"),
    projects = make("section", "card"),
    queue = make("section", "card");
  const accountActions = make("div", "row");
  accountActions.append(button("Создать проект", createDialog, true));
  if (!sourceClient)
    accountActions.append(
      button("Выйти", async () => {
        try {
          await request("/auth/logout", {});
          location.reload();
        } catch (error) {
          notice(message, describe(error), true);
        }
      }),
    );
  toolbar.append(make("h2", "", "Ваши проекты"), accountActions);
  grid.append(projects, queue);
  root.append(toolbar, message, grid);
  queue.append(
    make("p", "empty", "Выберите проект, чтобы открыть его обращения."),
  );
  function select(project) {
    const head = make("div", "row spread");
    head.append(
      make("h2", "", project.name),
      button("Подключение", () => integration(project)),
    );
    const box = make("div");
    queue.replaceChildren(head, box);
    inbox(box, project.id, ownerCall, { owner: true });
  }
  async function list(cursor) {
    try {
      const result = await ownerCall("projects_list", {
        limit: 20,
        ...(cursor ? { cursor } : {}),
      });
      if (!cursor) projects.replaceChildren();
      if (!result.projects.length && !cursor)
        projects.append(
          make(
            "p",
            "empty",
            "Создайте первый проект и получите ссылку для обратной связи.",
          ),
        );
      for (const project of result.projects) {
        const node = button("", () => select(project));
        node.className = "ticket";
        node.append(
          make("span", "project-name", project.name),
          make(
            "span",
            "badge",
            project.publicSubmit ? "Открытая форма" : "Закрытый проект",
          ),
        );
        projects.append(node);
      }
      if (result.nextCursor)
        projects.append(
          button("Показать ещё", (event) => {
            event.currentTarget.remove();
            void list(result.nextCursor);
          }),
        );
    } catch (error) {
      notice(message, describe(error), true);
    }
  }
  function createDialog() {
    const dialog = make("dialog", "modal"),
      form = make("form"),
      name = input("Например, Сайт студии"),
      website = input("https://example.com", "url"),
      publicSubmit = input("", "checkbox"),
      info = make("p", "notice");
    name.required = true;
    name.maxLength = 100;
    publicSubmit.checked = true;
    const checkLabel = make("label", "check");
    checkLabel.append(
      publicSubmit,
      make("span", "", "Разрешить отправку по публичной ссылке"),
    );
    const submit = make("button", "primary", "Создать проект");
    submit.type = "submit";
    let intent;
    form.append(
      make("h2", "", "Новый проект"),
      field("Название", name),
      field("Адрес сайта · необязательно", website),
      checkLabel,
      info,
      make("div", "row"),
    );
    form.lastChild.append(
      submit,
      button("Отмена", () => dialog.close()),
    );
    dialog.append(form);
    document.body.append(dialog);
    dialog.addEventListener("close", () => dialog.remove());
    dialog.showModal();
    name.focus();
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      intent ??= {
        name: name.value,
        website: website.value.trim(),
        publicSubmit: publicSubmit.checked,
        requestId: crypto.randomUUID(),
      };
      submit.disabled = true;
      name.disabled = website.disabled = publicSubmit.disabled = true;
      try {
        const value = await ownerCall("project_create", intent);
        dialog.close();
        await list();
        select(value.project);
        await integration(value.project);
      } catch (error) {
        notice(info, describe(error), true);
        submit.disabled = false;
        submit.textContent = "Проверить создание";
      }
    });
  }
  async function integration(project) {
    try {
      const value = await ownerCall("integration_get", {
          projectId: project.id,
        }),
        dialog = make("dialog", "modal"),
        info = make("p", "notice");
      dialog.append(
        make("h2", "", project.name),
        make("p", "", "Ссылка формы"),
        code(value.formUrl),
        button("Скопировать ссылку", () => copy(value.formUrl, info)),
        make("p", "", "Кнопка для сайта"),
        code(value.html),
        button("Скопировать код", () => copy(value.html, info)),
        make(
          "p",
          "muted",
          "Вставьте код в страницу сайта. Разрешите домен сервиса в CSP. Приватные ключи и токены в коде отсутствуют.",
        ),
        info,
        make("div", "row"),
      );
      dialog.lastChild.append(
        link("Открыть форму", value.formUrl, "button"),
        button("Закрыть", () => dialog.close()),
      );
      document.body.append(dialog);
      dialog.addEventListener("close", () => dialog.remove());
      dialog.showModal();
    } catch (error) {
      notice(message, describe(error), true);
    }
  }
  await list();
}

try {
  const match = /^\/p\/(fp_[a-f0-9]{32})$/.exec(location.pathname);
  if (match) await publicForm(match[1]);
  else if (location.pathname === "/guide") guide();
  else await dashboard();
} catch (error) {
  root.replaceChildren(
    hero(
      "Не удалось открыть страницу",
      "Проверьте адрес проекта или войдите в нужный аккаунт.",
    ),
    make("p", "notice error", describe(error)),
    link("На главную", "/", "button"),
  );
} finally {
  root.setAttribute("aria-busy", "false");
}
