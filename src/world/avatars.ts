import { el, safeImageUrl } from './dom';
import type { WorldApi } from './types';
import { canonicalWebP } from './image-codec.mjs';

interface AvatarResponse { profileId: string; avatarUrl: string; avatarRevision: number }

/** Private data is only retained in this instance and cleared at each permission context change. */
export class AvatarHydrator {
  private readonly api: WorldApi;
  private readonly roots = new Set<HTMLElement>();
  private readonly cache = new Map<string, string | null>();
  private readonly queue = new Map<string,{id:string;communityId:string|undefined;nodes:Set<HTMLElement>}>();
  private readonly nodeKeys=new WeakMap<HTMLElement,string>();
  private readonly observer: IntersectionObserver;
  private readonly mutations: MutationObserver;
  private communityId: string | undefined;
  private generation = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private destroyed = false;
  private loading = false;

  constructor(api: WorldApi, root: HTMLElement) {
    this.api = api;
    this.observer = new IntersectionObserver(entries => {
      for (const entry of entries) if (entry.isIntersecting) { this.observer.unobserve(entry.target); this.enqueue(entry.target as HTMLElement); }
    }, { rootMargin: '60px' });
    this.mutations = new MutationObserver(records => {
      for (const record of records){if(record.type==='attributes'&&record.target instanceof HTMLElement)this.scan(record.target);for (const node of record.addedNodes) if (node instanceof HTMLElement) this.scan(node);}
    });
    this.observe(root);
  }

  observe(root: HTMLElement): void {
    this.roots.add(root); this.watch(root); this.scan(root);
  }

  setContext(communityId?: string): void {
    this.communityId = communityId; this.generation++; this.cache.clear(); this.queue.clear(); this.loading = false;
    if (this.timer) clearTimeout(this.timer); this.timer = null;
    this.observer.disconnect(); this.mutations.disconnect();
    for (const root of this.roots) if (root.isConnected) { root.querySelectorAll<HTMLElement>('.sw-avatar[data-profile-id]').forEach(node => node.replaceChildren(el('span', '', node.dataset.avatarLabel ?? 'С'))); this.watch(root); this.scan(root); } else this.roots.delete(root);
  }

  destroy(): void {
    this.destroyed = true; this.generation++; if (this.timer) clearTimeout(this.timer);
    this.observer.disconnect(); this.mutations.disconnect(); this.roots.clear(); this.queue.clear(); this.cache.clear();
  }

  private scan(root: HTMLElement): void {
    const observe=(node:HTMLElement)=>{const key=this.cacheKey(node);if(this.nodeKeys.get(node)!==key){node.replaceChildren(el('span','',node.dataset.avatarLabel??'С'));this.nodeKeys.set(node,key);}this.observer.observe(node);};
    if (root.matches('.sw-avatar[data-profile-id]')) observe(root);
    root.querySelectorAll<HTMLElement>('.sw-avatar[data-profile-id]').forEach(observe);
  }
  private watch(root:HTMLElement):void{this.mutations.observe(root,{childList:true,subtree:true,attributes:true,attributeFilter:['data-avatar-community','data-profile-id','data-avatar-revision']});}

  private context(node:HTMLElement):string|undefined {
    const scoped=node.closest<HTMLElement>('[data-avatar-community]')?.dataset.avatarCommunity;
    return scoped&&/^[A-Za-z0-9_-]{3,160}$/u.test(scoped)?scoped:this.communityId;
  }

  private cacheKey(node:HTMLElement):string{return JSON.stringify([this.context(node)??null,node.dataset.profileId,node.dataset.avatarRevision]);}

  private enqueue(node: HTMLElement): void {
    const id = node.dataset.profileId; if (!id || !node.isConnected) return;
    const communityId=this.context(node),key=this.cacheKey(node);
    if (this.cache.has(key)) { this.apply(node, this.cache.get(key) ?? null); return; }
    const requestKey=JSON.stringify([communityId??null,id]),queued=this.queue.get(requestKey)??{id,communityId,nodes:new Set<HTMLElement>()};queued.nodes.add(node);this.queue.set(requestKey,queued);
    if (!this.timer && !this.loading) this.timer = setTimeout(() => { this.timer = null; void this.flush(); }, 30);
  }

  private apply(node: HTMLElement, value: string | null): void {
    if (!value || !node.isConnected) return;
    const url = safeImageUrl(value); if (!url || !url.startsWith('data:image/')) return;
    const image = el('img'); image.src = url; image.alt = ''; image.decoding = 'async'; node.replaceChildren(image);
  }

  private async flush(): Promise<void> {
    if (this.destroyed || this.loading || !this.queue.size) return;
    this.loading = true; const generation = this.generation;
    // One authority context per request; the same person can appear in two rooms.
    const communityId=this.queue.values().next().value!.communityId;
    const items=[...this.queue.entries()].filter(([,entry])=>entry.communityId===communityId).slice(0,24).map(([key,entry])=>{
      this.queue.delete(key);return[entry.id,[...entry.nodes].map(node=>({node,revision:node.dataset.avatarRevision}))] as const;
    });
    try {
      const result = await this.api.request<{ avatars: AvatarResponse[] }>('world.profile.avatars', { profileIds: items.map(([id]) => id), ...(communityId ? { communityId } : {}) });
      if (this.destroyed || generation !== this.generation) return;
      for (const [id, nodes] of items) {
        const avatar = result.avatars.find(value => value.profileId === id);
        for (const { node, revision } of nodes) {
          if (!node.isConnected||node.dataset.profileId !== id || node.dataset.avatarRevision !== revision||this.context(node)!==communityId) continue;
          this.cache.set(this.cacheKey(node), avatar?.avatarUrl ?? null);
          if (this.cache.size > 256) this.cache.delete(this.cache.keys().next().value!);
          if (avatar?.avatarUrl) this.apply(node, avatar.avatarUrl); else node.replaceChildren(el('span', '', node.dataset.avatarLabel ?? 'С'));
        }
      }
    } catch { /* Initials remain usable on an image fetch failure. No private image is persisted. */ }
    finally { if (generation === this.generation) { this.loading = false; if (this.queue.size) this.timer = setTimeout(() => { this.timer = null; void this.flush(); }, 30); } }
  }
}

export async function prepareAvatar(file: File): Promise<{ avatarUrl: string; thumbnailUrl: string }> {
  if (!['image/png', 'image/jpeg', 'image/webp'].includes(file.type)) throw Object.assign(new Error('Unsupported image'), { code: 'invalid_avatar_mime' });
  if (file.size > 20 * 1024 * 1024) throw Object.assign(new Error('Image is too large'), { code: 'avatar_too_large' });
  const bitmap = await createImageBitmap(file);
  try {
    const crop = Math.min(bitmap.width, bitmap.height);
    if (!crop) throw Object.assign(new Error('Image has no pixels'), { code: 'avatar_dimensions' });
    const encode = (dimension: number, limit: number): string => {
      const canvas = document.createElement('canvas'); canvas.width = dimension; canvas.height = dimension;
      const context = canvas.getContext('2d'); if (!context) throw new Error('Canvas unavailable');
      context.fillStyle = '#f4efe4'; context.fillRect(0, 0, dimension, dimension);
      context.drawImage(bitmap, (bitmap.width - crop) / 2, (bitmap.height - crop) / 2, crop, crop, 0, 0, dimension, dimension);
      for (let quality = .86; quality >= .22; quality -= .08) {
        const encoded = canvas.toDataURL('image/webp', quality);
        const binary = atob(encoded.split(',')[1] ?? '');
        const bytes = canonicalWebP(Uint8Array.from(binary, character => character.charCodeAt(0)));
        if (bytes.length <= limit) {
          let content = ''; for (let offset = 0; offset < bytes.length; offset += 8192) content += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
          return `data:image/webp;base64,${btoa(content)}`;
        }
      }
      if (dimension > 64) return encode(Math.round(dimension * .75), limit);
      throw Object.assign(new Error('Image encoding exceeds limit'), { code: 'avatar_too_large' });
    };
    return { avatarUrl: encode(512, 98_304), thumbnailUrl: encode(192, 8_192) };
  } finally { bitmap.close(); }
}
