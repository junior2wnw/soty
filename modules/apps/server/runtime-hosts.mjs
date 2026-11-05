import { createHash } from 'node:crypto';
import { assertApps, textId, appId, runtimePath } from './protocol.mjs';
import { validDnsHostname } from './domain-policy.mjs';
import { canonical } from '../scoped-embed/profile.mjs';

const widgetPattern = /^widget-([a-f0-9]{8})-([a-f0-9]{4})-([a-f0-9]{4})-([a-f0-9]{4})-([a-f0-9]{12})$/u;
const integer = value => Number.isSafeInteger(value) && value >= 1;
const denied = value => assertApps(value, 'app_widget_runtime_changed', 403);

/** Public routing selector only. Neither the hostname nor this fingerprint grants access. */
export function parseWidgetRuntimeHost(hostname) {
  if (typeof hostname !== 'string' || !validDnsHostname(hostname)) return null;
  const match = /^app-w([a-f0-9]{32})-([a-f0-9]{24})\.(.+)$/u.exec(hostname);
  if (!match) return null;
  const id = match[1];
  return Object.freeze({ widgetId: `widget-${id.slice(0,8)}-${id.slice(8,12)}-${id.slice(12,16)}-${id.slice(16,20)}-${id.slice(20)}`,
    scopeHash: match[2], suffix: match[3] });
}

function derivedRuntimeOrigin(prefix,identifier,scopeHash,zoneOrigin) {
  const zone = new URL(zoneOrigin);
  denied(typeof scopeHash === 'string' && /^[a-f0-9]{24}$/u.test(scopeHash));
  denied(zone.origin === zoneOrigin && !zone.username && !zone.password && validDnsHostname(zone.hostname)
    && (zone.protocol === 'https:' || zone.protocol === 'http:' && (zone.hostname === 'localhost' || zone.hostname.endsWith('.localhost'))));
  const host = `app-${prefix}${identifier}-${scopeHash}.${zone.hostname}`;
  denied(validDnsHostname(host));
  return `${zone.protocol}//${host}${zone.port ? ':' + zone.port : ''}`;
}
export function widgetRuntimeOrigin(widgetId,scopeHash,zoneOrigin) {
  const widget=widgetPattern.exec(widgetId);denied(widget);
  return derivedRuntimeOrigin('w',widget.slice(1).join(''),scopeHash,zoneOrigin);
}
export function parseMainRuntimeHost(hostname) {
  if(typeof hostname!=='string'||!validDnsHostname(hostname))return null;
  const match=/^app-m([a-f0-9]{32})-([a-f0-9]{24})\.(.+)$/u.exec(hostname);
  return match?Object.freeze({appId:'app-'+match[1],scopeHash:match[2],suffix:match[3]}):null;
}
export function mainRuntimeOrigin(id,scopeHash,zoneOrigin) {
  appId(id);return derivedRuntimeOrigin('m',id.slice(4),scopeHash,zoneOrigin);
}
export function captureMainRuntimeScope(decision,path) {
  denied(decision?.subject==='account');textId(decision.actor?.accountId);textId(decision.actor?.deviceId);appId(decision.appId);textId(decision.domainId);
  denied(integer(decision.policyEpoch)&&integer(decision.targetRevision)&&integer(decision.requiredBindingVersion)
    &&/^[a-f0-9]{64}$/u.test(decision.targetDigest)&&typeof decision.profile==='string'&&decision.profile.length<=128);
  runtimePath(path);const origin=new URL(decision.origin);denied(origin.origin===decision.origin&&!origin.username&&!origin.password);
  return Object.freeze({schema:'soty.main-runtime-scope.v1',accountId:decision.actor.accountId,deviceId:decision.actor.deviceId,appId:decision.appId,instanceId:'main',policyEpoch:decision.policyEpoch,
    entry:Object.freeze({domainId:decision.domainId,origin:decision.origin,path}),target:Object.freeze({revision:decision.targetRevision,digest:decision.targetDigest,profile:decision.profile,bindingFloor:decision.requiredBindingVersion})});
}

/** Copy only the current canonical binding; no Actor, cookie, password or Source ACK. */
export function captureWidgetRuntimeScope(decision, widget, path) {
  denied(decision?.subject === 'account' && widget && widgetPattern.test(widget.widgetId) && widget.state === 'active'
    && integer(widget.revision) && widget.appId === decision.appId && widget.entry?.domainId === decision.domainId
    && widget.entry.path === path && widget.source?.revision === decision.targetRevision
    && widget.source.digest === decision.targetDigest && widget.source.profile === decision.profile);
  textId(decision.actor?.accountId); textId(decision.actor?.deviceId); appId(decision.appId); textId(decision.domainId);
  denied(integer(decision.policyEpoch) && integer(decision.targetRevision) && integer(decision.requiredBindingVersion)
    && typeof decision.targetDigest === 'string' && /^[a-f0-9]{64}$/u.test(decision.targetDigest)
    && typeof decision.profile === 'string' && decision.profile.length <= 128);
  runtimePath(path);
  const origin = new URL(decision.origin); denied(origin.origin === decision.origin && !origin.username && !origin.password);
  return Object.freeze({ schema: 'soty.widget-runtime-scope.v1', accountId: decision.actor.accountId, deviceId: decision.actor.deviceId,
    widgetId: widget.widgetId, widgetRevision: widget.revision, appId: decision.appId, policyEpoch: decision.policyEpoch,
    entry: Object.freeze({ domainId: decision.domainId, origin: decision.origin, path }),
    target: Object.freeze({ revision: decision.targetRevision, digest: decision.targetDigest, profile: decision.profile,
      bindingFloor: decision.requiredBindingVersion }) });
}
export const widgetRuntimeScopeDigest = scope => createHash('sha256').update(canonical(scope)).digest('hex');
