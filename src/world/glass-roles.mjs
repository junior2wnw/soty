/** Structural UI semantics only. No app IDs, source records or network access. */
const COMPOSITE_FIELDS='.uf-global-search,.sw-inbox-search,.sw-chat-search,.sw-composer';
const NAVIGATION='.uf-canvas-dock,.uf-object-focus-bar,.sw-topbar,.cp-dock,.sw-chat-header,.sw-human-call-strip';
const PANELS='.uf-search-results,.sw-chat-menu,.se-message-menu,.sw-pwa-banner,[role="menu"],.action-sheet,.pair-screen,.qr-sheet,.access-sheet,.traffic-sheet';
const CARDS='.uf-preview,.sw-profile-card,.sw-community-card,.sw-empty,.sw-toast,.sw-message-body,.se-message-body,.sw-app-settings-source-result,.sw-app-settings-pending,.sw-app-settings-confirm,.sw-human-signin,.sw-ai-pending,.sw-ai-secret,.sw-ai-confirm';
const CHOICES='.sw-audience-choice,.sw-app-connect-audience label,.sw-view-choice';
export const SOTY_GLASS_SELECTOR=`button,summary,input,textarea,select,dialog,a.sw-button,[role="button"],${NAVIGATION},${COMPOSITE_FIELDS},${PANELS},${CARDS},${CHOICES}`;

export function sotyGlassRole(node) {
 if(!node?.matches||node.closest('[data-glass-ignore],.uf-plane,.sa-runtime,.sa-frame'))return null;
 const tag=node.tagName?.toLowerCase();
 if(tag==='dialog')return node.matches('.workspace-dialog')?null:'overlay';
 if(node.matches(NAVIGATION))return'navigation';
 if(node.matches(COMPOSITE_FIELDS))return'field';
 if(tag==='button'||tag==='summary'||node.getAttribute('role')==='button'||node.matches('a.sw-button')){
  if(node.matches('.sw-button-primary,.uf-canvas-add'))return'accent';
  // A toolbar owns one shared glass surface; its native icon controls stay
  // clickable without becoming another row of glass capsules.
  return node.parentElement?.closest(`${NAVIGATION},.glass-ui__action-list`)?null:'control';
 }
 if(['input','textarea','select'].includes(tag)){
  if(node.closest(COMPOSITE_FIELDS)!==null||tag==='input'&&['hidden','checkbox','radio','file','range'].includes(node.type))return null;
  return'field';
 }
 if(node.matches(CHOICES))return'control';
 if(node.matches(PANELS))return'panel';
 if(node.matches(CARDS))return'card';
 return null;
}
