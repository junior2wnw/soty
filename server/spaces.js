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
    summary: "заметки, планы, окружение"
  },
  {
    slug: "club",
    title: "Клуб",
    summary: "сообщество вокруг темы"
  }
]);

export function attachSpaces(app, { dataDir } = {}) {
  const metaStore = createMetaStore(dataDir);
  const photoStore = createPhotoStore(dataDir);
  const postStore = createPostStore(dataDir);
  const reviewStore = createReviewStore(dataDir);
  app.get("/api/spaces/:handle", async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(await publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, req.params.handle || ""));
  });
  app.get("/api/spaces/:handle/:space", async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(await publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, req.params.handle || "", req.params.space || ""));
  });
  app.post("/api/spaces/:handle/profile", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpaceMeta(metaStore, req, res);
  });
  app.post("/api/spaces/:handle/posts", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpacePost(postStore, req, res);
  });
  app.post("/api/spaces/:handle/photo", express.json({ limit: "3mb" }), async (req, res) => {
    await saveSpacePhoto(photoStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/photo", express.json({ limit: "3mb" }), async (req, res) => {
    await saveSpacePhoto(photoStore, req, res);
  });
  app.post("/api/spaces/:handle/reviews", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpaceReview(reviewStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/reviews", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpaceReview(reviewStore, req, res);
  });
  app.get("/manifest/space/:handle.json", async (req, res) => {
    await sendSpaceManifest(metaStore, photoStore, postStore, reviewStore, res, req.params.handle || "");
  });
  app.get("/manifest/space/:handle/:space.json", async (req, res) => {
    await sendSpaceManifest(metaStore, photoStore, postStore, reviewStore, res, req.params.handle || "", req.params.space || "");
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

async function publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, rawHandle, rawSpace = "") {
  const handle = cleanSlug(rawHandle) || "guest";
  const spaceSlug = cleanSlug(rawSpace);
  const meta = await readSpaceMeta(metaStore, handle);
  const ownerName = meta.displayName || titleFromSlug(handle);
  const activeSpace = spaceSlug ? spaceFor(spaceSlug) : null;
  const displayName = activeSpace ? `${activeSpace.title} · ${ownerName}` : ownerName;
  const url = activeSpace ? `/@${handle}/${activeSpace.slug}` : `/@${handle}`;
  const photo = await readSpacePhoto(photoStore, handle);
  const storedPosts = activeSpace ? [] : await readSpacePosts(postStore, handle);
  const reviews = await readSpaceReviews(reviewStore, handle, activeSpace?.slug || "");
  return {
    schema: "soty.personal-space.v1",
    kind: activeSpace ? "space" : "entity",
    handle,
    slug: activeSpace?.slug || "",
    url,
    displayName,
    shortName: activeSpace?.title || ownerName,
    accountName: ownerName,
    photoUrl: photo ? `/photo/space/${encodeURIComponent(handle)}.jpg?v=${photo.version}` : "",
    title: activeSpace ? "пространство" : "страница",
    headline: activeSpace
      ? activeSpace.summary
      : meta.headline || meta.about || "Визитка, отзывы, связь.",
    about: activeSpace
      ? `${activeSpace.title}: связь, отзывы, действия.`
      : meta.about || "Описание не указано.",
    accent: meta.accent || "#f1f1f1",
    contacts: [
      { label: "чат", value: `@${handle}`, href: `/?pwa=1&bare=1&to=${encodeURIComponent(`@${handle}`)}` },
      ...meta.contact ? [meta.contact] : [],
      { label: "страница", value: url, href: url }
    ],
    posts: storedPosts,
    reviews,
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

async function sendSpaceManifest(metaStore, photoStore, postStore, reviewStore, res, rawHandle, rawSpace = "") {
  const profile = await publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, rawHandle, rawSpace);
  const appName = manifestAppName(profile);
  const iconSrc = profile.photoUrl || (profile.slug
    ? `/icon/space/${encodeURIComponent(profile.handle)}/${encodeURIComponent(profile.slug)}.svg`
    : `/icon/space/${encodeURIComponent(profile.handle)}.svg`);
  const iconType = profile.photoUrl ? "image/jpeg" : "image/svg+xml";
  const iconSizes = profile.photoUrl ? ["192x192", "512x512"] : ["any"];
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/manifest+json; charset=utf-8");
  res.json({
    name: appName,
    short_name: manifestShortName(appName),
    description: profile.slug ? profile.displayName : profile.headline,
    id: profile.url,
    start_url: profile.url,
    scope: "/",
    display: "standalone",
    launch_handler: {
      client_mode: "navigate-existing"
    },
    background_color: "#cacaca",
    theme_color: "#000000",
    icons: iconSizes.map((sizes) => ({
      src: iconSrc,
      sizes,
      type: iconType,
      purpose: "any"
    }))
  });
}

function manifestAppName(profile) {
  const raw = profile.slug
    ? profile.displayName
    : profile.accountName || profile.displayName;
  return cleanReviewText(raw, 96) || "Соты";
}

function manifestShortName(value) {
  const chars = Array.from(cleanReviewText(value, 96));
  return chars.slice(0, 18).join("") || "Соты";
}

async function saveSpacePost(postStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const post = normalizePostBody(req.body);
  if (!post) {
    res.status(400).json({ ok: false, error: "invalid_post" });
    return;
  }
  const entries = await readSpacePosts(postStore, handle);
  const next = [{
    id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    title: post.title,
    text: post.text,
    meta: "Я",
    createdAt: new Date().toISOString()
  }, ...entries].slice(0, 50);
  await writeSpacePosts(postStore, handle, next);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, post: next[0] });
}

async function saveSpaceMeta(metaStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const meta = normalizeMetaBody(req.body);
  if (!meta) {
    res.status(400).json({ ok: false, error: "invalid_profile" });
    return;
  }
  await writeSpaceMeta(metaStore, handle, meta);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, profile: meta });
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

async function saveSpaceReview(reviewStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const review = normalizeReviewBody(req.body);
  if (!review) {
    res.status(400).json({ ok: false, error: "invalid_review" });
    return;
  }
  const entries = await readSpaceReviews(reviewStore, handle, spaceSlug);
  const next = [{
    id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    author: review.author,
    text: review.text,
    rating: review.rating,
    createdAt: new Date().toISOString()
  }, ...entries].slice(0, 50);
  await writeSpaceReviews(reviewStore, handle, spaceSlug, next);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, review: next[0] });
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
    summary: "большое место"
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

function createMetaStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-meta")
  };
}

function createPhotoStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-photos")
  };
}

function createPostStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-posts")
  };
}

function createReviewStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-reviews")
  };
}

function metaPath(metaStore, handle) {
  return path.join(metaStore.dir, `${handle}.json`);
}

function photoPath(photoStore, handle) {
  return path.join(photoStore.dir, `${handle}.json`);
}

function postPath(postStore, handle) {
  return path.join(postStore.dir, `${handle}.json`);
}

function reviewPath(reviewStore, handle, spaceSlug = "") {
  const suffix = spaceSlug ? `__${spaceSlug}` : "";
  return path.join(reviewStore.dir, `${handle}${suffix}.json`);
}

async function readSpaceMeta(metaStore, handle) {
  try {
    return normalizeStoredMeta(JSON.parse(await readFile(metaPath(metaStore, handle), "utf8"))) || {};
  } catch {
    return {};
  }
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

async function readSpacePosts(postStore, handle) {
  try {
    const records = JSON.parse(await readFile(postPath(postStore, handle), "utf8"));
    if (!Array.isArray(records)) {
      return [];
    }
    return records.map(normalizeStoredPost).filter(Boolean).slice(0, 50);
  } catch {
    return [];
  }
}

async function readSpaceReviews(reviewStore, handle, spaceSlug = "") {
  try {
    const records = JSON.parse(await readFile(reviewPath(reviewStore, handle, spaceSlug), "utf8"));
    if (!Array.isArray(records)) {
      return [];
    }
    return records.map(normalizeStoredReview).filter(Boolean).slice(0, 50);
  } catch {
    return [];
  }
}

async function writeSpaceMeta(metaStore, handle, meta) {
  await mkdir(metaStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(metaPath(metaStore, handle), JSON.stringify(meta, null, 2), { encoding: "utf8", mode: 0o600 });
}

async function writeSpacePhoto(photoStore, handle, photo) {
  await mkdir(photoStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(photoPath(photoStore, handle), JSON.stringify({
    version: Date.now().toString(36),
    bytes: photo.bytes.toString("base64")
  }), { encoding: "utf8", mode: 0o600 });
}

async function writeSpacePosts(postStore, handle, posts) {
  await mkdir(postStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(postPath(postStore, handle), JSON.stringify(posts, null, 2), { encoding: "utf8", mode: 0o600 });
}

async function writeSpaceReviews(reviewStore, handle, spaceSlug, reviews) {
  await mkdir(reviewStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(reviewPath(reviewStore, handle, spaceSlug), JSON.stringify(reviews, null, 2), { encoding: "utf8", mode: 0o600 });
}

function normalizePostBody(body) {
  const text = cleanReviewText(body?.text, 420);
  if (text.length < 2) {
    return null;
  }
  const title = cleanReviewText(body?.title, 120) || titleFromPostText(text);
  return { title, text };
}

function normalizeStoredPost(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  const text = cleanReviewText(record.text, 420);
  if (!text) {
    return null;
  }
  return {
    id: cleanReviewText(record.id, 80) || text,
    title: cleanReviewText(record.title, 120) || titleFromPostText(text),
    text,
    meta: cleanReviewText(record.meta, 80) || "Я",
    createdAt: cleanReviewText(record.createdAt, 40)
  };
}

function titleFromPostText(text) {
  return cleanReviewText(text, 54).replace(/[.!?…,:;]+$/u, "") || "Запись";
}

function normalizeMetaBody(body) {
  const displayName = cleanReviewText(body?.displayName, 100);
  const about = cleanReviewText(body?.about, 420);
  const contactText = cleanReviewText(body?.contact, 160);
  const contact = normalizeContactMeta(contactText);
  if (!displayName && !about && !contact) {
    return null;
  }
  return {
    ...(displayName ? { displayName, headline: about || "визитка, записи, отзывы, связь" } : {}),
    ...(about ? { about, headline: about } : {}),
    ...(contact ? { contact } : {})
  };
}

function normalizeStoredMeta(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  const displayName = cleanReviewText(record.displayName, 100);
  const about = cleanReviewText(record.about, 420);
  const headline = cleanReviewText(record.headline, 180);
  const contact = normalizeContactMeta(record.contact);
  return {
    ...(displayName ? { displayName } : {}),
    ...(about ? { about } : {}),
    ...(headline ? { headline } : {}),
    ...(contact ? { contact } : {})
  };
}

function normalizeContactMeta(value) {
  if (value && typeof value === "object") {
    const label = cleanReviewText(value.label, 32) || "контакт";
    const contactValue = cleanReviewText(value.value, 160);
    if (!contactValue) {
      return null;
    }
    const href = cleanContactHref(value.href || contactValue);
    return {
      label,
      value: contactValue,
      ...(href ? { href } : {})
    };
  }
  const text = cleanReviewText(value, 160);
  if (!text) {
    return null;
  }
  const href = cleanContactHref(text);
  return {
    label: "контакт",
    value: text,
    ...(href ? { href } : {})
  };
}

function cleanContactHref(value) {
  const text = cleanReviewText(value, 240);
  if (!text || /[<>"']/u.test(text)) {
    return "";
  }
  if (/^mailto:[^\s@]+@[^\s@]+\.[^\s@]+$/iu.test(text)) {
    return text;
  }
  if (/^https?:\/\//iu.test(text) || text.startsWith("/")) {
    return text;
  }
  if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(text)) {
    return `mailto:${text}`;
  }
  return "";
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

function normalizeReviewBody(body) {
  const text = cleanReviewText(body?.text, 320);
  if (text.length < 2) {
    return null;
  }
  return {
    author: cleanReviewText(body?.author, 80) || "гость",
    text,
    rating: cleanRating(body?.rating)
  };
}

function normalizeStoredReview(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  const text = cleanReviewText(record.text, 320);
  if (!text) {
    return null;
  }
  return {
    id: cleanReviewText(record.id, 80) || text,
    author: cleanReviewText(record.author, 80) || "гость",
    text,
    rating: cleanRating(record.rating),
    createdAt: cleanReviewText(record.createdAt, 40)
  };
}

function cleanReviewText(value, max) {
  return String(typeof value === "string" || typeof value === "number" ? value : "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function cleanRating(value) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(1, Math.min(5, Math.round(number))) : 5;
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
