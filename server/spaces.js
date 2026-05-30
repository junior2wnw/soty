const defaultSpaces = Object.freeze([
  {
    slug: "work",
    title: "Работа",
    summary: "услуги, задачи, результаты"
  },
  {
    slug: "home",
    title: "Дом",
    summary: "личные заметки, близкие, планы"
  },
  {
    slug: "club",
    title: "Клуб",
    summary: "люди вокруг темы"
  }
]);

export function attachSpaces(app) {
  app.get("/api/spaces/:handle", (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(publicSpaceProfile(req.params.handle || ""));
  });
  app.get("/api/spaces/:handle/:space", (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(publicSpaceProfile(req.params.handle || "", req.params.space || ""));
  });
  app.get("/manifest/space/:handle.json", (req, res) => {
    sendSpaceManifest(res, req.params.handle || "");
  });
  app.get("/manifest/space/:handle/:space.json", (req, res) => {
    sendSpaceManifest(res, req.params.handle || "", req.params.space || "");
  });
  app.get("/icon/space/:handle.svg", (req, res) => {
    sendSpaceIcon(res, req.params.handle || "");
  });
  app.get("/icon/space/:handle/:space.svg", (req, res) => {
    sendSpaceIcon(res, req.params.handle || "", req.params.space || "");
  });
}

function publicSpaceProfile(rawHandle, rawSpace = "") {
  const handle = cleanSlug(rawHandle) || "guest";
  const spaceSlug = cleanSlug(rawSpace);
  const ownerName = titleFromSlug(handle);
  const activeSpace = spaceSlug ? spaceFor(spaceSlug) : null;
  const displayName = activeSpace ? `${activeSpace.title} · ${ownerName}` : ownerName;
  const accent = colorFor(handle, spaceSlug);
  const url = activeSpace ? `/@${handle}/${activeSpace.slug}` : `/@${handle}`;
  return {
    schema: "soty.personal-space.v1",
    kind: activeSpace ? "space" : "person",
    handle,
    slug: activeSpace?.slug || "",
    url,
    displayName,
    shortName: activeSpace?.title || ownerName,
    title: activeSpace ? "пространство" : "личное пространство",
    headline: activeSpace
      ? activeSpace.summary
      : "визитка, личное место и приватная сота в одном простом экране",
    about: activeSpace
      ? `Здесь ${ownerName} собирает людей, сообщения, отзывы и полезные действия вокруг темы "${activeSpace.title}".`
      : `${ownerName} может начать с простой визитки, а потом раскрыть страницу в личное пространство, отзывы, сообщения и большое место.`,
    accent,
    contacts: [
      { label: "сообщение", value: `@${handle}`, href: `/?pwa=1&bare=1&to=${encodeURIComponent(`@${handle}`)}` },
      { label: "страница", value: url, href: url },
      { label: "контакт", value: `hello-${handle}@soty.local` }
    ],
    posts: [
      {
        id: "hello",
        title: activeSpace ? "Что здесь происходит" : "Первое впечатление",
        text: activeSpace
          ? "Короткое описание, закрепленные материалы, заявки и сообщения живут рядом, без ощущения технического пульта."
          : "Человек открывает QR и сразу видит понятную карточку: кто перед ним, чем полезен, как написать и где оставить отзыв.",
        meta: "сегодня"
      },
      {
        id: "grow",
        title: "Рост без перегруза",
        text: "Сначала визитка. Потом записи. Потом отзывы. Потом личка. Потом полноценное место для людей, задач и мини-приложений.",
        meta: "маршрут"
      }
    ],
    reviews: [
      {
        id: "trust",
        author: "Клиент",
        text: "Понятно, кто это, куда писать и что уже сделано. Не надо разбираться в кнопках.",
        rating: 5
      },
      {
        id: "speed",
        author: "Партнер",
        text: "Открыл QR, написал, получил ответ и файл в одном месте.",
        rating: 5
      }
    ],
    spaces: defaultSpaces.map((space) => ({
      ...space,
      href: `/@${handle}/${space.slug}`,
      active: space.slug === activeSpace?.slug
    })),
    actions: {
      messageUrl: `/?pwa=1&bare=1&to=${encodeURIComponent(`@${handle}`)}`,
      runtimeUrl: `/?pwa=1&space=${encodeURIComponent(url)}`
    }
  };
}

function sendSpaceManifest(res, rawHandle, rawSpace = "") {
  const profile = publicSpaceProfile(rawHandle, rawSpace);
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/manifest+json; charset=utf-8");
  res.json({
    name: `${profile.displayName} · Соты`,
    short_name: profile.shortName.slice(0, 12) || "Соты",
    id: profile.url,
    start_url: profile.url,
    scope: "/",
    display: "standalone",
    launch_handler: {
      client_mode: "navigate-existing"
    },
    background_color: "#f6f8fb",
    theme_color: profile.accent,
    icons: [
      {
        src: profile.slug
          ? `/icon/space/${encodeURIComponent(profile.handle)}/${encodeURIComponent(profile.slug)}.svg`
          : `/icon/space/${encodeURIComponent(profile.handle)}.svg`,
        sizes: "any",
        type: "image/svg+xml",
        purpose: "any maskable"
      }
    ]
  });
}

function sendSpaceIcon(res, rawHandle, rawSpace = "") {
  const profile = publicSpaceProfile(rawHandle, rawSpace);
  const initials = profile.shortName.slice(0, 2).toUpperCase();
  const safeInitials = escapeSvg(initials || "С");
  res.setHeader("Cache-Control", "public, max-age=3600");
  res.setHeader("Content-Type", "image/svg+xml; charset=utf-8");
  res.send(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <rect width="512" height="512" rx="112" fill="#f8f7f2"/>
  <circle cx="256" cy="226" r="154" fill="${profile.accent}"/>
  <path d="M114 380c34-54 80-82 142-82s108 28 142 82" fill="none" stroke="#171717" stroke-width="28" stroke-linecap="round"/>
  <text x="256" y="250" text-anchor="middle" font-family="Arial, sans-serif" font-size="118" font-weight="800" fill="#171717">${safeInitials}</text>
</svg>`);
}

function spaceFor(slug) {
  const known = defaultSpaces.find((space) => space.slug === slug);
  if (known) {
    return known;
  }
  return {
    slug,
    title: titleFromSlug(slug),
    summary: "личное большое место"
  };
}

function titleFromSlug(value) {
  return value
    .replace(/[-_.]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .replace(/^\p{Ll}/u, (char) => char.toLocaleUpperCase("ru-RU")) || "Соты";
}

function cleanSlug(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/^@/u, "")
    .replace(/[^\p{L}\p{N}._-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 64)
    .toLowerCase();
}

function colorFor(handle, space) {
  const palette = ["#78e08f", "#6fd5f6", "#ffb86b", "#ff7d8a", "#a7d46f", "#8fb7ff"];
  const text = `${handle}/${space || ""}`;
  let hash = 0;
  for (let index = 0; index < text.length; index += 1) {
    hash = (hash * 31 + text.charCodeAt(index)) >>> 0;
  }
  return palette[hash % palette.length];
}

function escapeSvg(value) {
  return String(value)
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;");
}
