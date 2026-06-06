import express from "express";
import { webcrypto } from "node:crypto";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { stableJson } from "trustlink-kernel";
import webPush from "web-push";

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

const runtimeModuleTargets = new Set(["apps", "actions", "access", "qr", "files", "chess"]);

export function attachSpaces(app, { dataDir } = {}) {
  const metaStore = createMetaStore(dataDir);
  const photoStore = createPhotoStore(dataDir);
  const postStore = createPostStore(dataDir);
  const reviewStore = createReviewStore(dataDir);
  const reactionStore = createReactionStore(dataDir);
  const messageStore = createMessageStore(dataDir);
  const moduleStore = createModuleStore(dataDir);
  const ownerStore = createOwnerStore(dataDir);
  const pushStore = createPushStore(dataDir);
  app.get("/api/push/vapid-public-key", async (_req, res) => {
    const keys = await readOrCreatePushKeys(pushStore);
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, publicKey: keys.publicKey });
  });
  app.post("/api/push/notices", express.json({ limit: "16kb" }), async (req, res) => {
    await pullPushNotices(pushStore, req, res);
  });
  app.get("/api/spaces/:handle", async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(await publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, reactionStore, moduleStore, ownerStore, req.params.handle || "", "", req.query?.viewer || ""));
  });
  app.get("/api/spaces/:handle/:space", async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(await publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, reactionStore, moduleStore, ownerStore, req.params.handle || "", req.params.space || "", req.query?.viewer || ""));
  });
  app.post("/api/spaces/:handle/profile", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpaceMeta(metaStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/posts", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpacePost(postStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/modules", express.json({ limit: "360kb" }), async (req, res) => {
    await saveSpaceModule(moduleStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/modules", express.json({ limit: "360kb" }), async (req, res) => {
    await saveSpaceModule(moduleStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/photo", express.json({ limit: "3mb" }), async (req, res) => {
    await saveSpacePhoto(photoStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/photo", express.json({ limit: "3mb" }), async (req, res) => {
    await saveSpacePhoto(photoStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/reviews", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpaceReview(reviewStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/reviews", express.json({ limit: "24kb" }), async (req, res) => {
    await saveSpaceReview(reviewStore, req, res);
  });
  app.post("/api/spaces/:handle/messages", express.json({ limit: "14mb" }), async (req, res) => {
    await saveSpaceMessage(messageStore, pushStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/messages", express.json({ limit: "14mb" }), async (req, res) => {
    await saveSpaceMessage(messageStore, pushStore, req, res);
  });
  app.post("/api/spaces/:handle/messages/thread", express.json({ limit: "12kb" }), async (req, res) => {
    await sendSpaceThread(messageStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/messages/thread", express.json({ limit: "12kb" }), async (req, res) => {
    await sendSpaceThread(messageStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/messages/reply", express.json({ limit: "14mb" }), async (req, res) => {
    await saveSpaceReply(messageStore, ownerStore, pushStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/messages/reply", express.json({ limit: "14mb" }), async (req, res) => {
    await saveSpaceReply(messageStore, ownerStore, pushStore, req, res);
  });
  app.post("/api/spaces/:handle/messages/react", express.json({ limit: "12kb" }), async (req, res) => {
    await saveSpaceMessageReaction(messageStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/messages/react", express.json({ limit: "12kb" }), async (req, res) => {
    await saveSpaceMessageReaction(messageStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/messages/inbox", express.json({ limit: "24kb" }), async (req, res) => {
    await sendSpaceInbox(messageStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/messages/inbox", express.json({ limit: "24kb" }), async (req, res) => {
    await sendSpaceInbox(messageStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/messages/push", express.json({ limit: "96kb" }), async (req, res) => {
    await saveSpacePushSubscription(pushStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/messages/push", express.json({ limit: "96kb" }), async (req, res) => {
    await saveSpacePushSubscription(pushStore, ownerStore, req, res);
  });
  app.post("/api/spaces/:handle/reactions", express.json({ limit: "8kb" }), async (req, res) => {
    await saveSpaceReaction(reactionStore, req, res);
  });
  app.post("/api/spaces/:handle/:space/reactions", express.json({ limit: "8kb" }), async (req, res) => {
    await saveSpaceReaction(reactionStore, req, res);
  });
  app.get("/manifest/space/:handle.json", async (req, res) => {
    await sendSpaceManifest(metaStore, photoStore, postStore, reviewStore, reactionStore, moduleStore, ownerStore, res, req.params.handle || "");
  });
  app.get("/manifest/space/:handle/:space.json", async (req, res) => {
    await sendSpaceManifest(metaStore, photoStore, postStore, reviewStore, reactionStore, moduleStore, ownerStore, res, req.params.handle || "", req.params.space || "");
  });
  app.get("/photo/space/:handle.jpg", async (req, res) => {
    await sendSpacePhoto(photoStore, res, req.params.handle || "");
  });
  app.get("/icon/space/:handle.svg", async (req, res) => {
    await sendSpaceIcon(photoStore, res, req.params.handle || "");
  });
  app.get("/icon/space/:handle/:space.svg", async (req, res) => {
    await sendSpaceIcon(photoStore, res, req.params.handle || "", req.params.space || "");
  });
}

async function publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, reactionStore, moduleStore, ownerStore, rawHandle, rawSpace = "", rawViewer = "") {
  const handle = cleanSlug(rawHandle) || "guest";
  const spaceSlug = cleanSlug(rawSpace);
  const viewer = cleanSlug(rawViewer);
  const meta = await readSpaceMeta(metaStore, handle);
  const owner = await readSpaceOwner(ownerStore, handle);
  const ownerName = meta.displayName || titleFromSlug(handle);
  const spaces = spacesForMeta(meta.spaces, handle, spaceSlug);
  const activeSpace = spaceSlug ? spaceFor(spaceSlug, spaces) : null;
  const displayName = activeSpace ? `${activeSpace.title} · ${ownerName}` : ownerName;
  const url = activeSpace ? `/@${handle}/${activeSpace.slug}` : `/@${handle}`;
  const photo = await readSpacePhoto(photoStore, handle);
  const storedPosts = activeSpace ? [] : await readSpacePosts(postStore, handle);
  const reviews = await readSpaceReviews(reviewStore, handle, activeSpace?.slug || "");
  const reactions = await readSpaceReactions(reactionStore, handle, activeSpace?.slug || "");
  const trustedHandles = cleanTrustedHandles(meta.trustedHandles);
  const trustedViewer = Boolean(viewer && (viewer === handle || trustedHandles.includes(viewer)));
  const modules = visibleSpaceModules(await readSpaceModules(moduleStore, handle, activeSpace?.slug || ""), trustedViewer);
  return {
    schema: "soty.personal-space.v1",
    kind: activeSpace ? "space" : "entity",
    handle,
    slug: activeSpace?.slug || "",
    url,
    ownerDeviceId: owner?.deviceId || "",
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
      { label: "чат", value: `@${handle}`, href: `${url}?layer=messages` },
      ...meta.contact ? [meta.contact] : [],
      { label: "страница", value: url, href: url }
    ],
    posts: storedPosts,
    reviews,
    reactions,
    trustedViewer,
    trustedHandles: viewer === handle ? trustedHandles : [],
    spaces,
    modules,
    actions: {
      messageUrl: `${url}?layer=messages`,
      runtimeUrl: `${url}?layer=place`
    }
  };
}

async function sendSpaceManifest(metaStore, photoStore, postStore, reviewStore, reactionStore, moduleStore, ownerStore, res, rawHandle, rawSpace = "") {
  const profile = await publicSpaceProfile(metaStore, photoStore, postStore, reviewStore, reactionStore, moduleStore, ownerStore, rawHandle, rawSpace);
  const appName = manifestAppName(profile);
  const appUrl = manifestAppUrl(profile);
  const baseIconSrc = profile.slug
    ? `/icon/space/${encodeURIComponent(profile.handle)}/${encodeURIComponent(profile.slug)}.svg`
    : `/icon/space/${encodeURIComponent(profile.handle)}.svg`;
  const iconVersion = manifestIconVersion(profile.photoUrl);
  const iconSrc = iconVersion ? `${baseIconSrc}?v=${encodeURIComponent(iconVersion)}` : baseIconSrc;
  const icons = [{
    src: iconSrc,
    sizes: "any",
    type: "image/svg+xml",
    purpose: "any"
  }];
  if (profile.photoUrl) {
    icons.push(
      {
        src: profile.photoUrl,
        sizes: "192x192",
        type: "image/jpeg",
        purpose: "any"
      },
      {
        src: profile.photoUrl,
        sizes: "512x512",
        type: "image/jpeg",
        purpose: "any"
      }
    );
  }
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/manifest+json; charset=utf-8");
  res.json({
    name: appName,
    short_name: manifestShortName(appName),
    description: profile.slug ? profile.displayName : profile.headline,
    id: appUrl,
    start_url: appUrl,
    scope: manifestScope(appUrl),
    display: "standalone",
    launch_handler: {
      client_mode: "navigate-existing"
    },
    background_color: "#cacaca",
    theme_color: "#000000",
    icons
  });
}

function manifestAppUrl(profile) {
  const handle = encodeURIComponent(profile.handle || "guest");
  return profile.slug
    ? `/pwa/@${handle}/${encodeURIComponent(profile.slug)}/`
    : `/pwa/@${handle}/`;
}

function manifestScope(url) {
  const pathOnly = String(url || "").split(/[?#]/u)[0];
  if (!pathOnly.startsWith("/pwa/@")) {
    return "/pwa/@guest/";
  }
  return pathOnly.endsWith("/") ? pathOnly : `${pathOnly}/`;
}

function manifestIconVersion(photoUrl) {
  const text = typeof photoUrl === "string" ? photoUrl : "";
  try {
    const url = new URL(text, "https://soty.local");
    return cleanVersion(url.searchParams.get("v") || "");
  } catch {
    return "";
  }
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

async function saveSpacePost(postStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const data = ownerActionData(req.body);
  const post = normalizePostBody(data);
  if (!post) {
    res.status(400).json({ ok: false, error: "invalid_post" });
    return;
  }
  if (!await authorizeSpaceOwner(ownerStore, req, res, "post", data)) {
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

async function saveSpaceModule(moduleStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const data = ownerActionData(req.body);
  const module = normalizeModuleBody(data);
  if (!module) {
    const serverHostedMiniApp = isServerHostedMiniAppBody(data);
    res.status(400).json({
      ok: false,
      error: serverHostedMiniApp ? "server_hosted_mini_apps_disabled" : "invalid_module"
    });
    return;
  }
  if (!await authorizeSpaceOwner(ownerStore, req, res, "module", data)) {
    return;
  }
  const entries = await readSpaceModules(moduleStore, handle, spaceSlug);
  const next = [{
    id: `module-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    ...module,
    createdAt: new Date().toISOString()
  }, ...entries.filter((entry) => moduleKey(entry) !== moduleKey(module))].slice(0, 50);
  await writeSpaceModules(moduleStore, handle, spaceSlug, next);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, module: next[0] });
}

async function saveSpaceMeta(metaStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const data = ownerActionData(req.body);
  const meta = normalizeMetaBody(data);
  if (!meta) {
    res.status(400).json({ ok: false, error: "invalid_profile" });
    return;
  }
  if (!await authorizeSpaceOwner(ownerStore, req, res, "profile", data)) {
    return;
  }
  await writeSpaceMeta(metaStore, handle, meta);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, profile: meta });
}

async function saveSpacePhoto(photoStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const data = ownerActionData(req.body);
  const photo = normalizePhotoBody(data);
  if (!photo) {
    res.status(400).json({ ok: false, error: "invalid_photo" });
    return;
  }
  if (!await authorizeSpaceOwner(ownerStore, req, res, "photo", data)) {
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

async function saveSpaceMessage(messageStore, pushStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const message = normalizeMessageBody(req.body);
  if (!message) {
    res.status(400).json({ ok: false, error: "invalid_message" });
    return;
  }
  const entries = await readSpaceMessages(messageStore, handle, spaceSlug);
  const stored = {
    id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    author: message.author,
    text: message.text,
    clientId: message.clientId,
    sender: "visitor",
    ...(message.replyTo ? { replyTo: message.replyTo } : {}),
    ...(message.attachment ? { attachment: message.attachment } : {}),
    reactions: {},
    createdAt: new Date().toISOString()
  };
  const next = [stored, ...entries].slice(0, 200);
  await writeSpaceMessages(messageStore, handle, spaceSlug, next);
  await mirrorDirectSpaceMessage(messageStore, {
    sourceHandle: handle,
    sourceSpaceSlug: spaceSlug,
    targetHandle: message.author,
    message: stored,
    sender: "owner",
    author: message.author
  });
  await notifySpaceMessageSubscribers(pushStore, handle, spaceSlug, {
    scope: "owner",
    title: message.author,
    body: messagePreviewText(stored),
    url: spaceMessagesUrl(handle, spaceSlug),
    icon: spacePushIconUrl(message.author),
    badge: "/icon.svg",
    tag: spacePushTag(handle, spaceSlug, message.clientId),
    renotify: true,
    vibrate: [24, 36, 24],
    timestamp: Date.now()
  });
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, message: publicMessage(stored) });
}

async function sendSpaceThread(messageStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const data = ownerActionData(req.body);
  const clientId = cleanMessageClientId(data?.clientId);
  if (!clientId) {
    res.status(400).json({ ok: false, error: "invalid_thread" });
    return;
  }
  const limit = safeThreadLimit(data?.limit);
  const reader = req.body?.owner ? "owner" : "visitor";
  const peek = data?.peek === true;
  if (reader === "owner" && !await authorizeSpaceOwner(ownerStore, req, res, "messages", data)) {
    return;
  }
  const entries = await readSpaceMessages(messageStore, handle, spaceSlug);
  const now = new Date().toISOString();
  let touched = false;
  const next = entries.map((message) => {
    if (message.clientId !== clientId) {
      return message;
    }
    if (!peek && reader === "owner" && message.sender !== "owner" && !message.readByOwnerAt) {
      touched = true;
      return { ...message, readByOwnerAt: now };
    }
    if (!peek && reader === "visitor" && message.sender === "owner" && !message.readByVisitorAt) {
      touched = true;
      return { ...message, readByVisitorAt: now };
    }
    return message;
  });
  if (touched) {
    await writeSpaceMessages(messageStore, handle, spaceSlug, next);
    await mirrorDirectSpaceThreadUpdates(messageStore, {
      sourceHandle: handle,
      sourceSpaceSlug: spaceSlug,
      targetHandle: directThreadPeerHandle(next, clientId, handle),
      clientId,
      messages: next
    });
  }
  const messages = next
    .filter((message) => message.clientId === clientId)
    .slice(0, limit)
    .reverse()
    .map(publicMessage)
    .filter(Boolean);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, messages });
}

async function saveSpaceReply(messageStore, ownerStore, pushStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const data = ownerActionData(req.body);
  const reply = normalizeMessageReplyBody(data);
  if (!reply) {
    res.status(400).json({ ok: false, error: "invalid_reply" });
    return;
  }
  if (!await authorizeSpaceOwner(ownerStore, req, res, "messages", data)) {
    return;
  }
  const entries = await readSpaceMessages(messageStore, handle, spaceSlug);
  const peerHandle = directThreadPeerHandle(entries, reply.clientId, handle);
  const stored = {
    id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    author: handle,
    text: reply.text,
    clientId: reply.clientId,
    sender: "owner",
    ...(reply.replyTo ? { replyTo: reply.replyTo } : {}),
    ...(reply.attachment ? { attachment: reply.attachment } : {}),
    reactions: {},
    createdAt: new Date().toISOString()
  };
  const next = [stored, ...entries].slice(0, 200);
  await writeSpaceMessages(messageStore, handle, spaceSlug, next);
  await mirrorDirectSpaceMessage(messageStore, {
    sourceHandle: handle,
    sourceSpaceSlug: spaceSlug,
    targetHandle: peerHandle,
    message: stored,
    sender: "visitor",
    author: handle
  });
  if (peerHandle) {
    await notifySpaceMessageSubscribers(pushStore, peerHandle, "", {
      scope: "owner",
      title: handle,
      body: messagePreviewText(stored),
      url: spaceMessagesUrl(peerHandle, ""),
      icon: spacePushIconUrl(handle, spaceSlug),
      badge: "/icon.svg",
      tag: spacePushTag(peerHandle, "", reply.clientId),
      renotify: true,
      vibrate: [24, 36, 24],
      timestamp: Date.now()
    });
  }
  await notifySpaceMessageSubscribers(pushStore, handle, spaceSlug, {
    scope: "visitor",
    clientId: reply.clientId,
    title: handle,
    body: messagePreviewText(stored),
    url: spaceMessagesUrl(handle, spaceSlug),
    icon: spacePushIconUrl(handle, spaceSlug),
    badge: "/icon.svg",
    tag: spacePushTag(handle, spaceSlug, reply.clientId),
    renotify: true,
    vibrate: [24, 36, 24],
    timestamp: Date.now()
  });
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, message: publicMessage(stored) });
}

async function saveSpaceMessageReaction(messageStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const data = ownerActionData(req.body);
  const reaction = normalizeMessageReactionBody(data);
  if (!reaction) {
    res.status(400).json({ ok: false, error: "invalid_reaction" });
    return;
  }
  let actorId = reaction.actorId;
  if (req.body?.owner) {
    if (!await authorizeSpaceOwner(ownerStore, req, res, "messages", data)) {
      return;
    }
    actorId = `owner:${handle}`;
  }
  const entries = await readSpaceMessages(messageStore, handle, spaceSlug);
  let changed = false;
  let target = null;
  const next = entries.map((message) => {
    if (message.id !== reaction.messageId || message.clientId !== reaction.clientId) {
      return message;
    }
    const reactionMap = normalizeMessageReactionMap(message.reactions);
    const current = new Set(reactionMap[reaction.key] || []);
    if (current.has(actorId)) {
      current.delete(actorId);
    } else {
      current.add(actorId);
    }
    const updatedMap = {
      ...reactionMap,
      [reaction.key]: Array.from(current).slice(0, 100)
    };
    if (updatedMap[reaction.key].length === 0) {
      delete updatedMap[reaction.key];
    }
    changed = true;
    target = {
      ...message,
      reactions: updatedMap
    };
    return target;
  });
  if (!changed || !target) {
    res.status(404).json({ ok: false, error: "message_not_found" });
    return;
  }
  await writeSpaceMessages(messageStore, handle, spaceSlug, next);
  await mirrorDirectSpaceMessageUpdate(messageStore, {
    sourceHandle: handle,
    sourceSpaceSlug: spaceSlug,
    targetHandle: directThreadPeerHandle(next, reaction.clientId, handle),
    message: target
  });
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, message: publicMessage(target) });
}

async function mirrorDirectSpaceMessage(messageStore, options) {
  const sourceHandle = cleanSlug(options?.sourceHandle || "");
  const sourceSpaceSlug = cleanSlug(options?.sourceSpaceSlug || "");
  const targetHandle = cleanSlug(options?.targetHandle || "");
  const message = normalizeStoredMessage({
    ...options?.message,
    author: options?.author,
    sender: options?.sender
  });
  if (!sourceHandle || sourceSpaceSlug || !targetHandle || targetHandle === sourceHandle || !message || !isDirectMessageClientId(message.clientId)) {
    return;
  }
  const entries = await readSpaceMessages(messageStore, targetHandle, "");
  if (entries.some((entry) => entry.id === message.id && entry.clientId === message.clientId)) {
    return;
  }
  await writeSpaceMessages(messageStore, targetHandle, "", [message, ...entries].slice(0, 200));
}

async function mirrorDirectSpaceMessageUpdate(messageStore, options) {
  const sourceHandle = cleanSlug(options?.sourceHandle || "");
  const sourceSpaceSlug = cleanSlug(options?.sourceSpaceSlug || "");
  const targetHandle = cleanSlug(options?.targetHandle || "");
  const message = normalizeStoredMessage(options?.message);
  if (!sourceHandle || sourceSpaceSlug || !targetHandle || targetHandle === sourceHandle || !message || !isDirectMessageClientId(message.clientId)) {
    return;
  }
  const entries = await readSpaceMessages(messageStore, targetHandle, "");
  let changed = false;
  const next = entries.map((entry) => {
    if (entry.id !== message.id || entry.clientId !== message.clientId) {
      return entry;
    }
    changed = true;
    return {
      ...entry,
      reactions: normalizeMessageReactionMap(message.reactions),
      ...(message.readByOwnerAt ? { readByOwnerAt: message.readByOwnerAt } : {}),
      ...(message.readByVisitorAt ? { readByVisitorAt: message.readByVisitorAt } : {})
    };
  });
  if (changed) {
    await writeSpaceMessages(messageStore, targetHandle, "", next);
  }
}

async function mirrorDirectSpaceThreadUpdates(messageStore, options) {
  const sourceHandle = cleanSlug(options?.sourceHandle || "");
  const sourceSpaceSlug = cleanSlug(options?.sourceSpaceSlug || "");
  const targetHandle = cleanSlug(options?.targetHandle || "");
  const clientId = cleanMessageClientId(options?.clientId);
  if (!sourceHandle || sourceSpaceSlug || !targetHandle || targetHandle === sourceHandle || !isDirectMessageClientId(clientId)) {
    return;
  }
  const updates = new Map();
  for (const rawMessage of Array.isArray(options?.messages) ? options.messages : []) {
    const message = normalizeStoredMessage(rawMessage);
    if (message?.clientId === clientId) {
      updates.set(message.id, message);
    }
  }
  if (updates.size === 0) {
    return;
  }
  const entries = await readSpaceMessages(messageStore, targetHandle, "");
  let changed = false;
  const next = entries.map((entry) => {
    const message = updates.get(entry.id);
    if (!message || message.clientId !== entry.clientId) {
      return entry;
    }
    changed = true;
    return {
      ...entry,
      reactions: normalizeMessageReactionMap(message.reactions),
      ...(message.readByOwnerAt ? { readByOwnerAt: message.readByOwnerAt } : {}),
      ...(message.readByVisitorAt ? { readByVisitorAt: message.readByVisitorAt } : {})
    };
  });
  if (changed) {
    await writeSpaceMessages(messageStore, targetHandle, "", next);
  }
}

function directThreadPeerHandle(entries, clientId, currentHandle) {
  const cleanClientId = cleanMessageClientId(clientId);
  const handle = cleanSlug(currentHandle || "");
  if (!cleanClientId || !isDirectMessageClientId(cleanClientId)) {
    return "";
  }
  for (const message of entries) {
    if (message.clientId !== cleanClientId || message.sender === "owner") {
      continue;
    }
    const author = cleanSlug(message.author || "");
    if (author && author !== handle) {
      return author;
    }
  }
  return "";
}

function isDirectMessageClientId(value) {
  return /^mc_dm_[a-z0-9_-]{8,72}$/iu.test(cleanMessageClientId(value));
}

async function sendSpaceInbox(messageStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const data = ownerActionData(req.body);
  if (!await authorizeSpaceOwner(ownerStore, req, res, "messages", data)) {
    return;
  }
  const limit = safeInboxLimit(data?.limit);
  const conversations = new Map();
  for (const message of await readSpaceMessages(messageStore, handle, spaceSlug)) {
    if (!message.clientId) {
      continue;
    }
    const current = conversations.get(message.clientId);
    if (!current) {
      conversations.set(message.clientId, {
        latest: message,
        visitorAuthor: message.sender === "owner" ? "" : message.author
      });
    } else if (!current.visitorAuthor && message.sender !== "owner") {
      current.visitorAuthor = message.author;
    }
  }
  const messages = Array.from(conversations.values())
    .slice(0, limit)
    .map((conversation) => publicMessage({
      ...conversation.latest,
      author: conversation.visitorAuthor || conversation.latest.author
    }))
    .filter(Boolean);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, messages });
}

async function saveSpacePushSubscription(pushStore, ownerStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const data = ownerActionData(req.body);
  const record = normalizePushRegistration(data);
  if (!record) {
    res.status(400).json({ ok: false, error: "invalid_push_subscription" });
    return;
  }
  if (record.scope === "owner" && !await authorizeSpaceOwner(ownerStore, req, res, "messages", data)) {
    return;
  }
  const now = new Date().toISOString();
  const entries = await readPushSubscriptions(pushStore, handle, spaceSlug);
  const current = entries.find((entry) => entry.endpoint === record.endpoint);
  const withoutCurrent = entries.filter((entry) => entry.endpoint !== record.endpoint);
  const next = [{
    ...record,
    notices: current?.notices || [],
    createdAt: current?.createdAt || now,
    updatedAt: now
  }, ...withoutCurrent].slice(0, 120);
  await writePushSubscriptions(pushStore, handle, spaceSlug, next);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true });
}

async function pullPushNotices(pushStore, req, res) {
  const endpoint = cleanPushEndpoint(req.body?.endpoint);
  if (!endpoint) {
    res.status(400).json({ ok: false, error: "invalid_endpoint" });
    return;
  }
  const notices = [];
  let files = [];
  try {
    files = await readdir(pushStore.dir);
  } catch {
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, notices });
    return;
  }
  for (const file of files) {
    if (!file.endsWith(".json") || file === "vapid.json" || file.includes("/") || file.includes("\\")) {
      continue;
    }
    const route = pushRouteFromFile(file);
    if (!route) {
      continue;
    }
    const entries = await readPushSubscriptions(pushStore, route.handle, route.spaceSlug);
    let changed = false;
    const next = entries.map((entry) => {
      if (entry.endpoint !== endpoint || entry.notices.length === 0) {
        return entry;
      }
      notices.push(...entry.notices);
      changed = true;
      return { ...entry, notices: [] };
    });
    if (changed) {
      await writePushSubscriptions(pushStore, route.handle, route.spaceSlug, next);
    }
  }
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, notices: notices.slice(-8).map(publicPushNotice).filter(Boolean) });
}

async function notifySpaceMessageSubscribers(pushStore, handle, spaceSlug, notice) {
  const cleanHandle = cleanSlug(handle || "");
  const cleanSpace = cleanSlug(spaceSlug || "");
  const cleanNotice = normalizePushNotice(notice);
  if (!cleanHandle || !cleanNotice) {
    return;
  }
  const keys = await readOrCreatePushKeys(pushStore);
  const entries = await readPushSubscriptions(pushStore, cleanHandle, cleanSpace);
  let changed = false;
  const storedNotice = {
    title: cleanNotice.title,
    body: cleanNotice.body,
    url: cleanNotice.url,
    ...(cleanNotice.icon ? { icon: cleanNotice.icon } : {}),
    ...(cleanNotice.badge ? { badge: cleanNotice.badge } : {}),
    ...(cleanNotice.tag ? { tag: cleanNotice.tag } : {}),
    ...(cleanNotice.renotify ? { renotify: true } : {}),
    ...(cleanNotice.vibrate.length ? { vibrate: cleanNotice.vibrate } : {}),
    ...(cleanNotice.timestamp ? { timestamp: cleanNotice.timestamp } : {}),
    createdAt: new Date().toISOString()
  };
  const queued = entries.map((entry) => {
    const matches = entry.scope === cleanNotice.scope
      && (entry.scope === "owner" || entry.clientId === cleanNotice.clientId);
    if (!matches) {
      return entry;
    }
    changed = true;
    return {
      ...entry,
      notices: [...entry.notices, storedNotice].slice(-5),
      updatedAt: new Date().toISOString()
    };
  });
  if (!changed) {
    return;
  }
  await writePushSubscriptions(pushStore, cleanHandle, cleanSpace, queued);
  const gone = new Set();
  const delivered = new Set();
  await Promise.all(queued
    .filter((entry) => entry.scope === cleanNotice.scope && (entry.scope === "owner" || entry.clientId === cleanNotice.clientId))
    .map(async (entry) => {
      const result = await sendNoticeWebPush(entry, keys, storedNotice);
      if (result === "gone") {
        gone.add(entry.endpoint);
      } else if (result === "sent") {
        delivered.add(entry.endpoint);
      }
    }));
  if (gone.size > 0 || delivered.size > 0) {
    await writePushSubscriptions(pushStore, cleanHandle, cleanSpace, queued
      .filter((entry) => !gone.has(entry.endpoint))
      .map((entry) => delivered.has(entry.endpoint)
        ? { ...entry, notices: entry.notices.filter((item) => item.createdAt !== storedNotice.createdAt) }
        : entry));
  }
}

async function sendNoticeWebPush(subscription, keys, notice) {
  const payload = publicPushNotice(notice);
  if (!payload || !keys?.privateJwk?.d) {
    return await sendEmptyWebPush(subscription, keys);
  }
  try {
    webPush.setVapidDetails(
      process.env.SOTY_WEB_PUSH_SUBJECT || "mailto:admin@soty.online",
      keys.publicKey,
      keys.privateJwk.d
    );
    await webPush.sendNotification({
      endpoint: subscription.endpoint,
      keys: subscription.keys
    }, JSON.stringify(payload), {
      TTL: 120,
      urgency: "high"
    });
    return "sent";
  } catch (error) {
    if (error?.statusCode === 404 || error?.statusCode === 410) {
      return "gone";
    }
    return await sendEmptyWebPush(subscription, keys);
  }
}

async function sendEmptyWebPush(subscription, keys) {
  try {
    const endpointUrl = new URL(subscription.endpoint);
    const audience = `${endpointUrl.protocol}//${endpointUrl.host}`;
    const jwt = await webPushJwt(keys, audience);
    const standard = await postWebPush(subscription.endpoint, {
      TTL: "120",
      Urgency: "high",
      Authorization: `WebPush ${jwt}`,
      "Crypto-Key": `p256ecdsa=${keys.publicKey}`
    });
    if (standard === "sent" || standard === "gone") {
      return standard;
    }
    return await postWebPush(subscription.endpoint, {
      TTL: "120",
      Urgency: "high",
      Authorization: `vapid t=${jwt}, k=${keys.publicKey}`
    });
  } catch {
    return "failed";
  }
}

async function postWebPush(endpoint, headers) {
  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers
    });
    if (response.status === 404 || response.status === 410) {
      return "gone";
    }
    return response.ok ? "sent" : "failed";
  } catch {
    return "failed";
  }
}

async function webPushJwt(keys, audience) {
  const header = base64UrlJson({ typ: "JWT", alg: "ES256" });
  const payload = base64UrlJson({
    aud: audience,
    exp: Math.floor(Date.now() / 1000) + 12 * 60 * 60,
    sub: process.env.SOTY_WEB_PUSH_SUBJECT || "mailto:admin@soty.online"
  });
  const unsigned = `${header}.${payload}`;
  const key = await webcrypto.subtle.importKey(
    "jwk",
    keys.privateJwk,
    { name: "ECDSA", namedCurve: "P-256" },
    false,
    ["sign"]
  );
  const signature = new Uint8Array(await webcrypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" },
    key,
    utf8Bytes(unsigned)
  ));
  return `${unsigned}.${base64UrlString(signature)}`;
}

function base64UrlJson(value) {
  return base64UrlString(utf8Bytes(JSON.stringify(value)));
}

async function saveSpaceReaction(reactionStore, req, res) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const spaceSlug = cleanSlug(req.params.space || "");
  const type = cleanReviewText(req.body?.type, 24) || "like";
  const clientId = cleanReactionClientId(req.body?.clientId);
  if (type !== "like" || !clientId) {
    res.status(400).json({ ok: false, error: "invalid_reaction" });
    return;
  }
  const record = await readSpaceReactionRecord(reactionStore, handle, spaceSlug);
  const existing = new Set(record.likes.map((item) => item.clientId));
  const nextLikes = existing.has(clientId)
    ? record.likes
    : [{ clientId, createdAt: new Date().toISOString() }, ...record.likes].slice(0, 5000);
  const next = { likes: nextLikes };
  await writeSpaceReactionRecord(reactionStore, handle, spaceSlug, next);
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, reactions: publicReactionStats(next) });
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

async function sendSpaceIcon(photoStore, res, rawHandle, rawSpace = "") {
  const profile = fallbackIconProfile(rawHandle, rawSpace);
  const initials = profile.shortName.slice(0, 2).toUpperCase();
  const safeInitials = escapeSvg(initials || "С");
  res.setHeader("Cache-Control", "public, max-age=3600");
  res.setHeader("Content-Type", "image/svg+xml; charset=utf-8");
  const handle = cleanSlug(rawHandle) || "guest";
  const photo = await readSpacePhoto(photoStore, handle);
  if (photo) {
    res.send(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <defs>
    <clipPath id="avatar">
      <rect width="512" height="512" rx="112"/>
    </clipPath>
  </defs>
  <rect width="512" height="512" rx="112" fill="#f8f7f2"/>
  <image width="512" height="512" href="data:image/jpeg;base64,${photo.bytes.toString("base64")}" preserveAspectRatio="xMidYMid slice" clip-path="url(#avatar)"/>
</svg>`);
    return;
  }
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

function spaceFor(slug, spaces = defaultSpaces) {
  const known = spaces.find((space) => space.slug === slug);
  if (known) {
    return known;
  }
  return {
    slug,
    title: titleFromSlug(slug),
    summary: "большое место"
  };
}

function spacesForMeta(value, handle, activeSlug = "") {
  const seen = new Set();
  const spaces = [];
  const add = (space) => {
    const slug = cleanSlug(space?.slug || "");
    const title = cleanReviewText(space?.title, 80) || titleFromSlug(slug);
    if (!slug || !title || seen.has(slug)) {
      return;
    }
    seen.add(slug);
    spaces.push({
      slug,
      title,
      summary: cleanReviewText(space?.summary, 140) || "пространство",
      href: `/@${handle}/${slug}`,
      active: slug === activeSlug
    });
  };
  defaultSpaces.forEach(add);
  if (Array.isArray(value)) {
    value.forEach(add);
  }
  return spaces.slice(0, 12);
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

function createReactionStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-reactions")
  };
}

function createMessageStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-messages")
  };
}

function createModuleStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-modules")
  };
}

function createOwnerStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-owners")
  };
}

function createPushStore(dataDir) {
  return {
    dir: path.join(dataDir || path.join(process.cwd(), "data"), "profile-push")
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

function reactionPath(reactionStore, handle, spaceSlug = "") {
  const suffix = spaceSlug ? `__${spaceSlug}` : "";
  return path.join(reactionStore.dir, `${handle}${suffix}.json`);
}

function messagePath(messageStore, handle, spaceSlug = "") {
  const suffix = spaceSlug ? `__${spaceSlug}` : "";
  return path.join(messageStore.dir, `${handle}${suffix}.json`);
}

function modulePath(moduleStore, handle, spaceSlug = "") {
  const suffix = spaceSlug ? `__${spaceSlug}` : "";
  return path.join(moduleStore.dir, `${handle}${suffix}.json`);
}

function ownerPath(ownerStore, handle) {
  return path.join(ownerStore.dir, `${handle}.json`);
}

function pushPath(pushStore, handle, spaceSlug = "") {
  const suffix = spaceSlug ? `__${spaceSlug}` : "";
  return path.join(pushStore.dir, `${handle}${suffix}.json`);
}

function pushKeysPath(pushStore) {
  return path.join(pushStore.dir, "vapid.json");
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

async function readSpaceReactionRecord(reactionStore, handle, spaceSlug = "") {
  try {
    return normalizeStoredReactionRecord(JSON.parse(await readFile(reactionPath(reactionStore, handle, spaceSlug), "utf8")));
  } catch {
    return { likes: [] };
  }
}

async function readSpaceReactions(reactionStore, handle, spaceSlug = "") {
  return publicReactionStats(await readSpaceReactionRecord(reactionStore, handle, spaceSlug));
}

async function readSpaceMessages(messageStore, handle, spaceSlug = "") {
  try {
    const records = JSON.parse(await readFile(messagePath(messageStore, handle, spaceSlug), "utf8"));
    if (!Array.isArray(records)) {
      return [];
    }
    return records.map(normalizeStoredMessage).filter(Boolean).slice(0, 200);
  } catch {
    return [];
  }
}

async function readSpaceModules(moduleStore, handle, spaceSlug = "") {
  try {
    const records = JSON.parse(await readFile(modulePath(moduleStore, handle, spaceSlug), "utf8"));
    if (!Array.isArray(records)) {
      return [];
    }
    return records.map(normalizeStoredModule).filter(Boolean).slice(0, 50);
  } catch {
    return [];
  }
}

function visibleSpaceModules(modules, trustedViewer) {
  return modules.filter((module) => module.visibility === "public" || trustedViewer);
}

async function readSpaceOwner(ownerStore, handle) {
  try {
    return normalizeStoredOwner(JSON.parse(await readFile(ownerPath(ownerStore, handle), "utf8")));
  } catch {
    return null;
  }
}

async function readPushSubscriptions(pushStore, handle, spaceSlug = "") {
  try {
    const records = JSON.parse(await readFile(pushPath(pushStore, handle, spaceSlug), "utf8"));
    if (!Array.isArray(records)) {
      return [];
    }
    return records.map(normalizePushRecord).filter(Boolean).slice(0, 120);
  } catch {
    return [];
  }
}

async function writePushSubscriptions(pushStore, handle, spaceSlug, records) {
  await mkdir(pushStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(pushPath(pushStore, handle, spaceSlug), JSON.stringify(records.slice(0, 120), null, 2), { encoding: "utf8", mode: 0o600 });
}

async function readOrCreatePushKeys(pushStore) {
  const envPublic = cleanBase64Url(process.env.SOTY_WEB_PUSH_PUBLIC_KEY || "", 256);
  const envPrivate = normalizePushPrivateJwk(safeJsonParse(process.env.SOTY_WEB_PUSH_PRIVATE_JWK || ""));
  if (envPublic && envPrivate) {
    return { publicKey: envPublic, privateJwk: envPrivate };
  }
  try {
    const stored = normalizePushKeys(JSON.parse(await readFile(pushKeysPath(pushStore), "utf8")));
    if (stored) {
      return stored;
    }
  } catch {
    // Create persistent VAPID keys below.
  }
  const keyPair = await webcrypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["sign", "verify"]
  );
  const publicBytes = new Uint8Array(await webcrypto.subtle.exportKey("raw", keyPair.publicKey));
  const privateJwk = await webcrypto.subtle.exportKey("jwk", keyPair.privateKey);
  const keys = normalizePushKeys({
    publicKey: base64UrlString(publicBytes),
    privateJwk
  });
  if (!keys) {
    throw new Error("failed_to_create_push_keys");
  }
  await mkdir(pushStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(pushKeysPath(pushStore), JSON.stringify(keys, null, 2), { encoding: "utf8", mode: 0o600 });
  return keys;
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

async function writeSpaceReactionRecord(reactionStore, handle, spaceSlug, reactions) {
  await mkdir(reactionStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(reactionPath(reactionStore, handle, spaceSlug), JSON.stringify(reactions, null, 2), { encoding: "utf8", mode: 0o600 });
}

async function writeSpaceMessages(messageStore, handle, spaceSlug, messages) {
  await mkdir(messageStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(messagePath(messageStore, handle, spaceSlug), JSON.stringify(messages, null, 2), { encoding: "utf8", mode: 0o600 });
}

async function writeSpaceModules(moduleStore, handle, spaceSlug, modules) {
  await mkdir(moduleStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(modulePath(moduleStore, handle, spaceSlug), JSON.stringify(modules, null, 2), { encoding: "utf8", mode: 0o600 });
}

async function writeSpaceOwner(ownerStore, owner) {
  await mkdir(ownerStore.dir, { recursive: true, mode: 0o700 });
  await writeFile(ownerPath(ownerStore, owner.handle), JSON.stringify(owner, null, 2), { encoding: "utf8", mode: 0o600 });
}

function ownerActionData(body) {
  return isPlainRecord(body?.data) ? body.data : body;
}

async function authorizeSpaceOwner(ownerStore, req, res, action, data) {
  const handle = cleanSlug(req.params.handle || "") || "guest";
  const slug = cleanSlug(req.params.space || "");
  const result = await verifyOwnerProof(req.body?.owner, { action, handle, slug, data });
  if (!result.ok) {
    res.status(401).json({ ok: false, error: result.error || "owner_signature_required" });
    return false;
  }
  const stored = await readSpaceOwner(ownerStore, handle);
  if (stored && (stored.deviceId !== result.owner.deviceId || stableJson(stored.publicJwk) !== stableJson(result.owner.publicJwk))) {
    res.status(403).json({ ok: false, error: "space_owned_by_another_device" });
    return false;
  }
  const now = new Date().toISOString();
  await writeSpaceOwner(ownerStore, {
    handle,
    deviceId: result.owner.deviceId,
    publicJwk: result.owner.publicJwk,
    createdAt: stored?.createdAt || now,
    updatedAt: now
  });
  return true;
}

async function verifyOwnerProof(proof, expected) {
  if (!isPlainRecord(proof) || !isPlainRecord(proof.payload) || typeof proof.signature !== "string") {
    return { ok: false, error: "owner_signature_required" };
  }
  const payload = proof.payload;
  if (payload.v !== 1 || payload.kind !== "soty.personal-space.owner-action") {
    return { ok: false, error: "invalid_owner_payload" };
  }
  const action = cleanOwnerAction(payload.action);
  const handle = cleanSlug(payload.handle || "");
  const slug = cleanSlug(payload.slug || "");
  if (action !== expected.action || handle !== expected.handle || slug !== expected.slug) {
    return { ok: false, error: "owner_route_mismatch" };
  }
  const publicJwk = normalizeOwnerPublicJwk(payload.publicJwk);
  if (!publicJwk) {
    return { ok: false, error: "invalid_owner_key" };
  }
  const deviceId = cleanOwnerDeviceId(payload.deviceId);
  const derivedDeviceId = await deriveOwnerDeviceId(publicJwk);
  if (!deviceId || deviceId !== derivedDeviceId) {
    return { ok: false, error: "owner_device_mismatch" };
  }
  if (cleanOwnerHash(payload.bodyHash) !== await ownerBodyHash(expected.data)) {
    return { ok: false, error: "owner_body_mismatch" };
  }
  if (!isFreshOwnerTimestamp(payload.createdAt)) {
    return { ok: false, error: "stale_owner_signature" };
  }
  const ok = await verifyOwnerSignature(publicJwk, payload, proof.signature);
  return ok
    ? { ok: true, owner: { deviceId, publicJwk } }
    : { ok: false, error: "invalid_owner_signature" };
}

async function verifyOwnerSignature(publicJwk, payload, signature) {
  try {
    const key = await webcrypto.subtle.importKey(
      "jwk",
      publicJwk,
      { name: "ECDSA", namedCurve: "P-256" },
      false,
      ["verify"]
    );
    return await webcrypto.subtle.verify(
      { name: "ECDSA", hash: "SHA-256" },
      key,
      base64UrlBytes(signature),
      utf8Bytes(stableJson(payload))
    );
  } catch {
    return false;
  }
}

async function deriveOwnerDeviceId(publicJwk) {
  const digest = await webcrypto.subtle.digest("SHA-256", utf8Bytes(stableJson(publicJwk)));
  return `dev_${base64UrlString(new Uint8Array(digest)).slice(0, 32)}`;
}

async function ownerBodyHash(data) {
  const digest = await webcrypto.subtle.digest("SHA-256", utf8Bytes(stableJson(data)));
  return base64UrlString(new Uint8Array(digest));
}

function isFreshOwnerTimestamp(value) {
  const time = Date.parse(String(value || ""));
  if (!Number.isFinite(time)) {
    return false;
  }
  const ageMs = Math.abs(Date.now() - time);
  return ageMs <= 24 * 60 * 60 * 1000;
}

function normalizeStoredOwner(record) {
  if (!isPlainRecord(record)) {
    return null;
  }
  const handle = cleanSlug(record.handle || "");
  const deviceId = cleanOwnerDeviceId(record.deviceId);
  const publicJwk = normalizeOwnerPublicJwk(record.publicJwk);
  if (!handle || !deviceId || !publicJwk) {
    return null;
  }
  return {
    handle,
    deviceId,
    publicJwk,
    createdAt: cleanReviewText(record.createdAt, 40) || new Date(0).toISOString(),
    updatedAt: cleanReviewText(record.updatedAt, 40) || new Date(0).toISOString()
  };
}

function normalizeOwnerPublicJwk(value) {
  if (!isPlainRecord(value)) {
    return null;
  }
  const kty = cleanReviewText(value.kty, 16);
  const crv = cleanReviewText(value.crv, 16);
  const x = cleanBase64Url(value.x, 120);
  const y = cleanBase64Url(value.y, 120);
  if (kty !== "EC" || crv !== "P-256" || !x || !y) {
    return null;
  }
  return {
    kty,
    crv,
    x,
    y,
    ext: value.ext === true,
    key_ops: Array.isArray(value.key_ops) ? value.key_ops.filter((item) => typeof item === "string").slice(0, 4) : undefined
  };
}

function cleanOwnerAction(value) {
  const action = cleanReviewText(value, 24);
  return action === "profile" || action === "post" || action === "photo" || action === "module" || action === "messages" ? action : "";
}

function cleanOwnerDeviceId(value) {
  const text = String(value || "").trim();
  return /^dev_[A-Za-z0-9_-]{16,80}$/u.test(text) ? text.slice(0, 120) : "";
}

function cleanOwnerHash(value) {
  return cleanBase64Url(value, 128);
}

function cleanBase64Url(value, max) {
  const text = String(value || "").trim().slice(0, max);
  return /^[A-Za-z0-9_-]+$/u.test(text) ? text : "";
}

function isPlainRecord(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function safeJsonParse(value) {
  try {
    return JSON.parse(String(value || ""));
  } catch {
    return null;
  }
}

function base64UrlBytes(value) {
  try {
    return Buffer.from(String(value || ""), "base64url");
  } catch {
    return Buffer.alloc(0);
  }
}

function base64UrlString(bytes) {
  return Buffer.from(bytes).toString("base64url");
}

function utf8Bytes(value) {
  return Buffer.from(String(value), "utf8");
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

function normalizeModuleBody(body) {
  const kind = cleanModuleKind(body?.kind);
  const title = cleanReviewText(body?.title, 80);
  const inlineHtml = kind === "miniapp" ? cleanMiniAppInlineHtml(body?.inlineHtml || body?.html) : "";
  const href = kind === "miniapp"
    ? inlineHtml
      ? "about:srcdoc"
      : cleanMiniAppHref(body?.href || body?.target)
    : kind === "runtime"
      ? cleanRuntimeModuleTarget(body?.href || body?.target)
      : cleanModuleHref(body?.href || body?.target);
  if (!title || !href) {
    return null;
  }
  return {
    kind,
    title,
    summary: cleanReviewText(body?.summary, 140),
    href,
    visibility: body?.visibility === "trusted" ? "trusted" : "public",
    ...(kind === "miniapp" ? { layout: cleanMiniAppLayout(body?.layout) } : {}),
    ...(inlineHtml ? { inlineHtml } : {})
  };
}

function normalizeStoredModule(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  const module = normalizeModuleBody(record);
  if (!module) {
    return null;
  }
  return {
    id: cleanReviewText(record.id, 80) || moduleKey(module),
    ...module,
    createdAt: cleanReviewText(record.createdAt, 40)
  };
}

function moduleKey(module) {
  if (module?.kind === "miniapp" && module.inlineHtml) {
    return `${module.kind}:inline:${module.title}`.toLowerCase();
  }
  return `${module.kind}:${module.href}`.toLowerCase();
}

function cleanModuleKind(value) {
  const kind = cleanReviewText(value, 24);
  return kind === "miniapp" || kind === "runtime" ? kind : "link";
}

function cleanRuntimeModuleTarget(value) {
  const target = cleanReviewText(value, 40);
  return runtimeModuleTargets.has(target) ? target : "";
}

function cleanModuleHref(value) {
  const text = cleanReviewText(value, 360);
  if (!text || /[<>"']/u.test(text)) {
    return "";
  }
  if (/^(?:\/|https?:\/\/|mailto:|tel:)/iu.test(text)) {
    return text;
  }
  return "";
}

function cleanMiniAppHref(value) {
  const text = cleanReviewText(value, 500);
  if (!text || /[\s<>"']/u.test(text) || isServerHostedMiniAppUrl(text)) {
    return "";
  }
  try {
    const url = new URL(text);
    if (url.protocol === "https:") {
      return url.toString().slice(0, 500);
    }
    if (url.protocol === "http:" && isLoopbackHost(url.hostname)) {
      return url.toString().slice(0, 500);
    }
  } catch {
    return "";
  }
  return "";
}

function cleanMiniAppInlineHtml(value) {
  const text = typeof value === "string"
    ? value
      .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
      .trim()
    : "";
  if (!text || text.length > 240000) {
    return "";
  }
  if (!/(?:<!doctype|<html|<body|<main|<section|<div|<script|<style)/iu.test(text)) {
    return "";
  }
  return text;
}

function isServerHostedMiniAppBody(body) {
  return cleanModuleKind(body?.kind) === "miniapp"
    && !cleanMiniAppInlineHtml(body?.inlineHtml || body?.html)
    && isServerHostedMiniAppUrl(body?.href || body?.target);
}

function isServerHostedMiniAppUrl(value) {
  const text = cleanReviewText(value, 500);
  if (!text) {
    return false;
  }
  if (text.startsWith("/") && !text.startsWith("//")) {
    return true;
  }
  try {
    const url = new URL(text);
    const host = url.hostname.toLowerCase();
    // Mini-app hosting under Soty is intentionally disabled for now; generated apps must live outside this web origin.
    return host === "xn--n1afe0b.online";
  } catch {
    return false;
  }
}

function isLoopbackHost(value) {
  const host = String(value || "").toLowerCase();
  return host === "localhost" || host === "127.0.0.1" || host === "::1" || host === "[::1]";
}

function cleanMiniAppLayout(value) {
  const layout = cleanReviewText(value, 24).toLowerCase().replace(/_/gu, "-");
  if (layout === "compact" || layout === "large" || layout === "full" || layout === "floating") {
    return layout;
  }
  return "half";
}

function titleFromPostText(text) {
  return cleanReviewText(text, 54).replace(/[.!?…,:;]+$/u, "") || "Запись";
}

function normalizeMetaBody(body) {
  const displayName = cleanReviewText(body?.displayName, 100);
  const about = cleanReviewText(body?.about, 420);
  const contactText = cleanReviewText(body?.contact, 160);
  const contact = normalizeContactMeta(contactText);
  const trustedHandles = Array.isArray(body?.trustedHandles) || typeof body?.trustedHandles === "string"
    ? cleanTrustedHandles(body.trustedHandles)
    : null;
  const spaces = Array.isArray(body?.spaces) ? normalizeSpacesMeta(body.spaces) : null;
  if (!displayName && !about && !contact && trustedHandles === null && spaces === null) {
    return null;
  }
  return {
    ...(displayName ? { displayName, headline: about || "визитка, записи, отзывы, связь" } : {}),
    ...(about ? { about, headline: about } : {}),
    ...(contact ? { contact } : {}),
    ...(spaces !== null ? { spaces } : {}),
    ...(trustedHandles !== null ? { trustedHandles } : {})
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
  const trustedHandles = cleanTrustedHandles(record.trustedHandles);
  const spaces = normalizeSpacesMeta(record.spaces);
  return {
    ...(displayName ? { displayName } : {}),
    ...(about ? { about } : {}),
    ...(headline ? { headline } : {}),
    ...(contact ? { contact } : {}),
    ...(spaces.length ? { spaces } : {}),
    ...(trustedHandles.length ? { trustedHandles } : {})
  };
}

function normalizeSpacesMeta(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  const seen = new Set();
  const spaces = [];
  for (const item of value) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const slug = cleanSlug(item.slug || "");
    const title = cleanReviewText(item.title, 80);
    if (!slug || !title || seen.has(slug)) {
      continue;
    }
    seen.add(slug);
    spaces.push({
      slug,
      title,
      summary: cleanReviewText(item.summary, 140)
    });
    if (spaces.length >= 12) {
      break;
    }
  }
  return spaces;
}

function cleanTrustedHandles(value) {
  const source = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(/[\s,;]+/u)
      : [];
  const seen = new Set();
  const result = [];
  for (const item of source) {
    const handle = cleanSlug(item);
    if (!handle || seen.has(handle)) {
      continue;
    }
    seen.add(handle);
    result.push(handle);
    if (result.length >= 50) {
      break;
    }
  }
  return result;
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

function normalizeMessageBody(body) {
  const text = cleanReviewText(body?.text, 420);
  const clientId = cleanMessageClientId(body?.clientId);
  const attachment = normalizeMessageAttachment(body?.attachment);
  const replyTo = normalizeMessageReplyRef(body?.replyTo);
  if ((!text && !attachment) || !clientId) {
    return null;
  }
  return {
    author: cleanReviewText(body?.author, 80) || "guest",
    text,
    clientId,
    ...(replyTo ? { replyTo } : {}),
    ...(attachment ? { attachment } : {})
  };
}

function normalizeMessageReplyBody(body) {
  const text = cleanReviewText(body?.text, 420);
  const clientId = cleanMessageClientId(body?.clientId);
  const attachment = normalizeMessageAttachment(body?.attachment);
  const replyTo = normalizeMessageReplyRef(body?.replyTo);
  if ((!text && !attachment) || !clientId) {
    return null;
  }
  return {
    text,
    clientId,
    ...(replyTo ? { replyTo } : {}),
    ...(attachment ? { attachment } : {})
  };
}

function normalizeMessageReactionBody(body) {
  const messageId = cleanReviewText(body?.messageId, 80);
  const clientId = cleanMessageClientId(body?.clientId);
  const actorId = cleanMessageReactionActorId(body?.actorId);
  const key = cleanMessageReactionKey(body?.key);
  if (!messageId || !clientId || !actorId || !key) {
    return null;
  }
  return {
    messageId,
    clientId,
    actorId,
    key
  };
}

function normalizePushRegistration(body) {
  if (!isPlainRecord(body)) {
    return null;
  }
  const scope = cleanReviewText(body.scope, 16) === "owner" ? "owner" : cleanReviewText(body.scope, 16) === "visitor" ? "visitor" : "";
  const clientId = scope === "visitor" ? cleanMessageClientId(body.clientId) : "";
  const subscription = normalizePushSubscription(body.subscription) || normalizePushSubscription(body);
  if (!scope || (scope === "visitor" && !clientId) || !subscription) {
    return null;
  }
  return {
    scope,
    clientId,
    endpoint: subscription.endpoint,
    keys: subscription.keys,
    title: cleanReviewText(body.title, 80),
    url: cleanPushUrl(body.url),
    notices: [],
    createdAt: new Date(0).toISOString(),
    updatedAt: new Date(0).toISOString()
  };
}

function normalizePushRecord(record) {
  if (!isPlainRecord(record)) {
    return null;
  }
  const registration = normalizePushRegistration(record);
  if (!registration) {
    return null;
  }
  return {
    ...registration,
    notices: Array.isArray(record.notices)
      ? record.notices.map(normalizeStoredPushNotice).filter(Boolean).slice(-5)
      : [],
    createdAt: cleanReviewText(record.createdAt, 40) || new Date(0).toISOString(),
    updatedAt: cleanReviewText(record.updatedAt, 40) || new Date(0).toISOString()
  };
}

function normalizePushSubscription(value) {
  if (!isPlainRecord(value)) {
    return null;
  }
  const endpoint = cleanPushEndpoint(value.endpoint);
  const keys = isPlainRecord(value.keys)
    ? {
        p256dh: cleanBase64Url(value.keys.p256dh, 256),
        auth: cleanBase64Url(value.keys.auth, 64)
      }
    : null;
  if (!endpoint || !keys?.p256dh || !keys.auth) {
    return null;
  }
  return { endpoint, keys };
}

function normalizePushNotice(value) {
  if (!isPlainRecord(value)) {
    return null;
  }
  const scope = cleanReviewText(value.scope, 16) === "owner" ? "owner" : cleanReviewText(value.scope, 16) === "visitor" ? "visitor" : "";
  const clientId = scope === "visitor" ? cleanMessageClientId(value.clientId) : "";
  const title = cleanReviewText(value.title, 80) || "Соты";
  const body = cleanReviewText(value.body, 180) || "Новое сообщение";
  const url = cleanPushUrl(value.url);
  const icon = cleanPushAssetUrl(value.icon);
  const badge = cleanPushAssetUrl(value.badge);
  const tag = cleanPushTag(value.tag);
  const vibrate = cleanPushVibrate(value.vibrate);
  const timestamp = cleanPushTimestamp(value.timestamp);
  const renotify = value.renotify === true;
  if (!scope || (scope === "visitor" && !clientId) || !url) {
    return null;
  }
  return {
    scope,
    clientId,
    title,
    body,
    url,
    icon,
    badge,
    tag,
    renotify,
    vibrate,
    timestamp
  };
}

function normalizeStoredPushNotice(value) {
  if (!isPlainRecord(value)) {
    return null;
  }
  const title = cleanReviewText(value.title, 80) || "Соты";
  const body = cleanReviewText(value.body, 180) || "Новое сообщение";
  const url = cleanPushUrl(value.url);
  const icon = cleanPushAssetUrl(value.icon);
  const badge = cleanPushAssetUrl(value.badge);
  const tag = cleanPushTag(value.tag);
  const vibrate = cleanPushVibrate(value.vibrate);
  const timestamp = cleanPushTimestamp(value.timestamp) || Date.parse(cleanReviewText(value.createdAt, 40) || "") || 0;
  if (!url) {
    return null;
  }
  return {
    title,
    body,
    url,
    ...(icon ? { icon } : {}),
    ...(badge ? { badge } : {}),
    ...(tag ? { tag } : {}),
    ...(value.renotify === true ? { renotify: true } : {}),
    ...(vibrate.length ? { vibrate } : {}),
    ...(timestamp ? { timestamp } : {}),
    createdAt: cleanReviewText(value.createdAt, 40) || new Date(0).toISOString()
  };
}

function publicPushNotice(value) {
  const notice = normalizeStoredPushNotice(value);
  return notice
    ? {
        title: notice.title,
        body: notice.body,
        url: notice.url,
        ...(notice.icon ? { icon: notice.icon } : {}),
        ...(notice.badge ? { badge: notice.badge } : {}),
        ...(notice.tag ? { tag: notice.tag } : {}),
        ...(notice.renotify ? { renotify: true } : {}),
        ...(notice.vibrate?.length ? { vibrate: notice.vibrate } : {}),
        ...(notice.timestamp ? { timestamp: notice.timestamp } : {})
      }
    : null;
}

function normalizeStoredMessage(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  const message = normalizeMessageBody(record);
  if (!message) {
    return null;
  }
  return {
    id: cleanReviewText(record.id, 80) || message.text,
    ...message,
    sender: cleanMessageSender(record.sender),
    reactions: normalizeMessageReactionMap(record.reactions),
    readByOwnerAt: cleanReviewText(record.readByOwnerAt, 40),
    readByVisitorAt: cleanReviewText(record.readByVisitorAt, 40),
    createdAt: cleanReviewText(record.createdAt, 40) || new Date(0).toISOString()
  };
}

function publicMessage(record) {
  const message = normalizeStoredMessage(record);
  if (!message) {
    return null;
  }
  return {
    id: message.id,
    author: message.author,
    text: message.text,
    clientId: message.clientId,
    sender: message.sender,
    ...(message.replyTo ? { replyTo: message.replyTo } : {}),
    ...(message.attachment ? { attachment: publicMessageAttachment(message.attachment) } : {}),
    reactions: publicMessageReactions(message.reactions),
    ...(message.readByOwnerAt ? { readByOwnerAt: message.readByOwnerAt } : {}),
    ...(message.readByVisitorAt ? { readByVisitorAt: message.readByVisitorAt } : {}),
    createdAt: message.createdAt
  };
}

function normalizeMessageReplyRef(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const id = cleanReviewText(value.id, 80);
  const text = cleanReviewText(value.text, 140);
  if (!id || !text) {
    return null;
  }
  return {
    id,
    author: cleanReviewText(value.author, 80) || "guest",
    text
  };
}

function normalizeMessageAttachment(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const name = cleanReviewText(value.name, 120) || "file";
  const type = cleanAttachmentType(value.type);
  const size = safeAttachmentSize(value.size);
  const dataUrl = cleanAttachmentDataUrl(value.dataUrl);
  if (!size || !dataUrl) {
    return null;
  }
  return {
    name,
    type,
    size,
    dataUrl
  };
}

function publicMessageAttachment(value) {
  const attachment = normalizeMessageAttachment(value);
  return attachment
    ? {
        name: attachment.name,
        type: attachment.type,
        size: attachment.size,
        dataUrl: attachment.dataUrl
      }
    : null;
}

function normalizeMessageReactionMap(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const result = {};
  for (const [rawKey, rawActors] of Object.entries(value)) {
    const key = cleanMessageReactionKey(rawKey);
    if (!key || !Array.isArray(rawActors)) {
      continue;
    }
    const actors = [];
    const seen = new Set();
    for (const rawActor of rawActors) {
      const actor = cleanMessageReactionActorId(rawActor);
      if (!actor || seen.has(actor)) {
        continue;
      }
      seen.add(actor);
      actors.push(actor);
      if (actors.length >= 100) {
        break;
      }
    }
    if (actors.length) {
      result[key] = actors;
    }
  }
  return result;
}

function publicMessageReactions(value) {
  const reactionMap = normalizeMessageReactionMap(value);
  const result = {};
  for (const [key, actors] of Object.entries(reactionMap)) {
    result[key] = actors.length;
  }
  return result;
}

function safeInboxLimit(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) ? Math.max(1, Math.min(50, number)) : 30;
}

function safeThreadLimit(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) ? Math.max(1, Math.min(100, number)) : 80;
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

function normalizeStoredReactionRecord(record) {
  if (!record || typeof record !== "object") {
    return { likes: [] };
  }
  const likes = Array.isArray(record.likes)
    ? record.likes
        .map(normalizeStoredReaction)
        .filter(Boolean)
        .slice(0, 5000)
    : [];
  return { likes };
}

function normalizeStoredReaction(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  const clientId = cleanReactionClientId(record.clientId);
  if (!clientId) {
    return null;
  }
  return {
    clientId,
    createdAt: cleanReviewText(record.createdAt, 40) || new Date(0).toISOString()
  };
}

function publicReactionStats(record) {
  const normalized = normalizeStoredReactionRecord(record);
  return {
    likes: normalized.likes.length
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

function cleanReactionClientId(value) {
  const text = cleanReviewText(value, 80);
  return /^rc_[a-z0-9_-]{8,76}$/iu.test(text) ? text : "";
}

function cleanMessageClientId(value) {
  const text = cleanReviewText(value, 80);
  return /^mc_[a-z0-9_-]{8,76}$/iu.test(text) ? text : "";
}

function cleanMessageReactionActorId(value) {
  const text = cleanReviewText(value, 96);
  return /^(mc_[a-z0-9_-]{8,76}|owner:[\p{L}\p{N}._-]{1,64})$/iu.test(text) ? text : "";
}

function cleanMessageReactionKey(value) {
  const text = cleanReviewText(value, 16);
  return text === "heart" || text === "check" || text === "like" ? text : "";
}

function cleanPushEndpoint(value) {
  const text = String(typeof value === "string" ? value : "").trim();
  if (text.length < 20 || text.length > 2048) {
    return "";
  }
  try {
    const url = new URL(text);
    return url.protocol === "https:" ? url.href : "";
  } catch {
    return "";
  }
}

function cleanPushUrl(value) {
  const text = cleanReviewText(value, 240) || "/?pwa=1";
  try {
    const url = new URL(text, "https://soty.local");
    return `${url.pathname}${url.search}${url.hash}`.slice(0, 240) || "/?pwa=1";
  } catch {
    return "/?pwa=1";
  }
}

function cleanPushAssetUrl(value) {
  const text = cleanReviewText(value, 240);
  if (!text) {
    return "";
  }
  try {
    const origin = "https://soty.local";
    const url = new URL(text, origin);
    if (url.origin !== origin) {
      return "";
    }
    return `${url.pathname}${url.search}`.slice(0, 240);
  } catch {
    return "";
  }
}

function cleanPushTag(value) {
  return cleanReviewText(value, 140)
    .replace(/[^\p{L}\p{N}:._/?=@-]+/gu, "-")
    .replace(/-+/gu, "-")
    .slice(0, 140);
}

function cleanPushVibrate(value) {
  const raw = Array.isArray(value) ? value : typeof value === "number" ? [value] : [];
  return raw
    .map((item) => Math.round(Number(item)))
    .filter((item) => Number.isFinite(item) && item >= 0)
    .map((item) => Math.min(item, 220))
    .slice(0, 7);
}

function cleanPushTimestamp(value) {
  const number = Math.round(Number(value));
  return Number.isFinite(number) && number > 0 && number < 4_102_444_800_000
    ? number
    : 0;
}

function normalizePushKeys(value) {
  if (!isPlainRecord(value)) {
    return null;
  }
  const publicKey = cleanBase64Url(value.publicKey, 256);
  const privateJwk = normalizePushPrivateJwk(value.privateJwk);
  return publicKey && privateJwk ? { publicKey, privateJwk } : null;
}

function normalizePushPrivateJwk(value) {
  if (!isPlainRecord(value)) {
    return null;
  }
  const kty = cleanReviewText(value.kty, 16);
  const crv = cleanReviewText(value.crv, 16);
  const x = cleanBase64Url(value.x, 120);
  const y = cleanBase64Url(value.y, 120);
  const d = cleanBase64Url(value.d, 120);
  if (kty !== "EC" || crv !== "P-256" || !x || !y || !d) {
    return null;
  }
  return { kty, crv, x, y, d, ext: true };
}

function cleanMessageSender(value) {
  const text = cleanReviewText(value, 24).toLowerCase();
  return text === "owner" ? "owner" : "visitor";
}

function cleanAttachmentType(value) {
  const text = cleanReviewText(value, 80).toLowerCase();
  return /^[a-z0-9][a-z0-9.+-]*\/[a-z0-9][a-z0-9.+-]*$/u.test(text) ? text : "application/octet-stream";
}

function safeAttachmentSize(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number > 0 && number <= 8_000_000 ? number : 0;
}

function cleanAttachmentDataUrl(value) {
  const text = String(typeof value === "string" ? value : "").trim();
  if (text.length < 20 || text.length > 11_200_000) {
    return "";
  }
  const match = text.match(/^data:([a-z0-9][a-z0-9.+-]*\/[a-z0-9][a-z0-9.+-]*);base64,([a-z0-9+/=]+)$/iu);
  if (!match) {
    return "";
  }
  const bytes = Buffer.from(match[2], "base64");
  if (bytes.length <= 0 || bytes.length > 8_000_000) {
    return "";
  }
  return text;
}

function pushRouteFromFile(file) {
  const name = String(file || "").replace(/\.json$/u, "");
  if (!name || name === "vapid" || name.includes("/") || name.includes("\\")) {
    return null;
  }
  const [rawHandle, rawSpace = ""] = name.split("__");
  const handle = cleanSlug(rawHandle);
  const spaceSlug = cleanSlug(rawSpace);
  return handle ? { handle, spaceSlug } : null;
}

function spaceMessagesUrl(handle, spaceSlug = "") {
  const encodedHandle = encodeURIComponent(cleanSlug(handle || "") || "guest");
  const encodedSpace = encodeURIComponent(cleanSlug(spaceSlug || ""));
  return encodedSpace
    ? `/@${encodedHandle}/${encodedSpace}?layer=messages`
    : `/@${encodedHandle}?layer=messages`;
}

function spacePushIconUrl(handle, spaceSlug = "") {
  const encodedHandle = encodeURIComponent(cleanSlug(handle || "") || "guest");
  const encodedSpace = encodeURIComponent(cleanSlug(spaceSlug || ""));
  return encodedSpace
    ? `/icon/space/${encodedHandle}/${encodedSpace}.svg`
    : `/icon/space/${encodedHandle}.svg`;
}

function spacePushTag(handle, spaceSlug = "", clientId = "") {
  const cleanHandle = cleanSlug(handle || "") || "guest";
  const cleanSpace = cleanSlug(spaceSlug || "");
  const cleanClient = cleanMessageClientId(clientId) || "thread";
  return cleanPushTag(["soty", "messages", cleanHandle, cleanSpace, cleanClient].filter(Boolean).join(":"));
}

function messagePreviewText(message) {
  const text = cleanReviewText(message?.text, 180);
  if (text) {
    return text;
  }
  const attachment = normalizeMessageAttachment(message?.attachment);
  return attachment?.name ? `Файл: ${attachment.name}` : "Новое сообщение";
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
