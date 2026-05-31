import express from "express";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

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

export function attachSpaces(app, { dataDir } = {}) {
  const photoStore = createPhotoStore(dataDir);
  app.get("/api/spaces/:handle", async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(await publicSpaceProfile(photoStore, req.params.handle || ""));
  });
  app.get("/api/spaces/:handle/:space", async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(await publicSpaceProfile(photoStore, req.params.handle || "", req.params.space || ""));
  });
  app.post("/api/spaces/:handle/photo", express.json({ limit: "3mb" }), async (req, res) => {
    await saveSpacePhoto(photoStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/photo", express.json({ limit: "3mb" }), async (req, res) => {
    await saveSpacePhoto(photoStore, req, res);
  });
  app.get("/manifest/space/:handle.json", async (req, res) => {
    await sendSpaceManifest(photoStore, res, req.params.handle || "");
  });
  app.get("/manifest/space/:handle/:space.json", async (req, res) => {
    await sendSpaceManifest(photoStore, res, req.params.handle || "", req.params.space || "");
  });
  app.get("/photo/space/:handle.jpg", async (req, res) => {
    await sendSpacePhoto(photoStore, res, req.params.handle || "");
  });
  app.get("/icon/space/:handle.svg", (req, res) => {
    sendSpaceIcon(res, req.params.handle || "");
  });
  app.get("/icon/space/:handle/:space.svg", (req, res) => {
    sendSpaceIcon(res, req.params.handle || "", req.params.space || "");
  });
}

async function publicSpaceProfile(photoStore, rawHandle, rawSpace = "") {
  const handle = cleanSlug(rawHandle) || "guest";
  const spaceSlug = cleanSlug(rawSpace);
  const ownerName = titleFromSlug(handle);
  const activeSpace = spaceSlug ? spaceFor(spaceSlug) : null;
  const displayName = activeSpace ? `${activeSpace.title} · ${ownerName}` : ownerName;
  const accent = colorFor(handle, spaceSlug);
  const url = activeSpace ? `/@${handle}/${activeSpace.slug}` : `/@${handle}`;
  const photo = await readSpacePhoto(photoStore, handle);
  return {
    schema: "soty.personal-space.v1",
    kind: activeSpace ? "space" : "person",
    handle,
    slug: activeSpace?.slug || "",
    url,
    displayName,
    shortName: activeSpace?.title || ownerName,
    accountName: ownerName,
    photoUrl: photo ? `/photo/space/${encodeURIComponent(handle)}.jpg?v=${photo.version}` : "",
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

async function sendSpaceManifest(photoStore, res, rawHandle, rawSpace = "") {
  const profile = await publicSpaceProfile(photoStore, rawHandle, rawSpace);
  const appName = profile.accountName || profile.displayName;
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/manifest+json; charset=utf-8");
  res.json({
    name: appName,
    short_name: appName.slice(0, 12) || "Соты",
    description: profile.slug ? profile.displayName : profile.headline,
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
        src: profile.photoUrl || (profile.slug
          ? `/icon/space/${encodeURIComponent(profile.handle)}/${encodeURIComponent(profile.slug)}.svg`
          : `/icon/space/${encodeURIComponent(profile.handle)}.svg`),
        sizes: profile.photoUrl ? "512x512" : "any",
        type: profile.photoUrl ? "image/jpeg" : "image/svg+xml",
        purpose: "any maskable"
      }
    ]
  });
}

async function saveSpacePhoto(photoStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const photo = normalizePhotoBody(req.body);
  if (!photo) {
    res.status(400).json({ ok: false, error: "invalid_photo" });
    return;
  }
  await writeSpacePhoto(photoStore, handle, photo);
  const saved = await readSpacePhoto(photoStore, handle);
  res.setHeader("Cache-Control", "no-store");
  res.json({
    ok: true,
    photoUrl: saved ? `/photo/space/${encodeURIComponent(handle)}.jpg?v=${saved.version}` : ""
  });
}

async function sendSpacePhoto(photoStore, res, rawHandle) {
  const handle = cleanSlug(rawHandle) || "guest";
  const photo = await readSpacePhoto(photoStore, handle);
  if (!photo) {
    res.status(404).json({ ok: false, error: "profile_photo_not_found" });
    return;
  }
  res.setHeader("Cache-Control", "public, max-age=3600");
  res.setHeader("Content-Type", "image/jpeg");
  res.send(photo.bytes);
}

function sendSpaceIcon(res, rawHandle, rawSpace = "") {
  const profile = fallbackIconProfile(rawHandle, rawSpace);
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

function fallbackIconProfile(rawHandle, rawSpace = "") {
  const handle = cleanSlug(rawHandle) || "guest";
  const spaceSlug = cleanSlug(rawSpace);
  const ownerName = titleFromSlug(handle);
  const activeSpace = spaceSlug ? spaceFor(spaceSlug) : null;
  return {
    shortName: activeSpace?.title || ownerName,
    accent: colorFor(handle, spaceSlug)
  };
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

function createPhotoStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-photos")
  };
}

function photoPath(photoStore, handle) {
  return path.join(photoStore.dir, `${handle}.json`);
}

async function readSpacePhoto(photoStore, handle) {
  try {
    const record = JSON.parse(await readFile(photoPath(photoStore, handle), "utf8"));
    if (!record || typeof record.bytes !== "string" || !/^[a-z0-9+/=]+$/iu.test(record.bytes)) {
      return null;
    }
    const bytes = Buffer.from(record.bytes, "base64");
    if (!bytes.length || bytes.length > 1_500_000) {
      return null;
    }
    return {
      bytes,
      version: cleanVersion(record.version || "")
    };
  } catch {
    return null;
  }
}

async function writeSpacePhoto(photoStore, handle, photo) {
  await mkdir(photoStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(photoPath(photoStore, handle), JSON.stringify({
    version: Date.now().toString(36),
    bytes: photo.bytes.toString("base64")
  }), { encoding: "utf8", mode: 0o600 });
}

function normalizePhotoBody(body) {
  const dataUrl = typeof body?.dataUrl === "string" ? body.dataUrl : "";
  const match = dataUrl.match(/^data:image\/jpeg;base64,([a-z0-9+/=]+)$/iu);
  if (!match?.[1]) {
    return null;
  }
  const bytes = Buffer.from(match[1], "base64");
  if (bytes.length < 200 || bytes.length > 1_500_000) {
    return null;
  }
  return { bytes };
}

function cleanVersion(value) {
  return String(value || "").replace(/[^a-z0-9_-]/giu, "").slice(0, 32) || "1";
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
