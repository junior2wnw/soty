(() => {
  const app = document.getElementById("app");
  const boot = document.querySelector("[data-html-boot]");
  const parts = window.location.pathname.split("/").filter(Boolean);
  const first = parts[0] || "";

  if (!app || !boot || !first.startsWith("@")) {
    boot?.remove();
    return;
  }

  let handle = first.slice(1) || "guest";
  try {
    handle = decodeURIComponent(handle);
  } catch {
    handle = "guest";
  }
  document.body.classList.add("personal-space-mode");
  document.body.dataset.sotyBoot = "1";
  boot.hidden = false;
  const label = boot.querySelector("[data-html-boot-handle]");
  if (label) {
    label.textContent = `@${handle}`;
  }

  const reloadKey = `soty:boot-reload:${window.location.pathname}${window.location.search}`;
  window.setTimeout(() => {
    const stillBooting = document.body.dataset.sotyReady !== "1"
      && document.querySelector("[data-html-boot]");
    let alreadyReloaded = false;
    try {
      alreadyReloaded = window.sessionStorage.getItem(reloadKey) === "1";
    } catch {
      alreadyReloaded = true;
    }
    if (!stillBooting || alreadyReloaded) {
      return;
    }
    try {
      window.sessionStorage.setItem(reloadKey, "1");
    } catch {
      return;
    }
    window.location.reload();
  }, 4200);
})();
