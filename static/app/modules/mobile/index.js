import { MOBILE_BREAKPOINT, STORAGE_KEYS } from "../../core/constants.js";

export function createMobileModule(ctx) {
  const panels = Array.from(document.querySelectorAll(".grid-stack-item[gs-id]"));
  const drawer = document.getElementById("mobileNavDrawer");
  const overlay = document.getElementById("mobileNavOverlay");
  const toggleBtn = document.getElementById("mobileNavToggleBtn");
  const closeBtn = document.getElementById("mobileNavCloseBtn");
  const menu = document.getElementById("mobilePanelMenu");
  const dashboardMain = document.querySelector(".dashboard-main");

  function isMobile() {
    return window.innerWidth <= MOBILE_BREAKPOINT;
  }

  function getHiddenSet() {
    const raw = ctx.store.getJSON(STORAGE_KEYS.layout, {}) || {};
    const hidden = Array.isArray(raw.hidden) ? raw.hidden : [];
    return new Set(hidden);
  }

  function getVisiblePanels() {
    const hidden = getHiddenSet();
    const raw = ctx.store.getJSON(STORAGE_KEYS.layout, {}) || {};
    const order = Array.isArray(raw.order) ? raw.order : panels.map((panel) => getPanelId(panel));
    const byId = new Map(panels.map((panel) => [getPanelId(panel), panel]));

    return order
      .map((id) => byId.get(id))
      .filter((panel) => panel && !hidden.has(getPanelId(panel)));
  }

  function getPanelId(panel) {
    return panel.getAttribute("gs-id") || "";
  }

  function getPanelMeta(panel) {
    const navTitle =
      panel.dataset.mobileNavTitle ||
      panel.querySelector("h3")?.textContent?.trim() ||
      panel.dataset.panelTitle ||
      getPanelId(panel);

    const fullTitle = panel.dataset.panelTitle || navTitle;

    return {
      id: getPanelId(panel),
      navTitle,
      fullTitle,
    };
  }

  function setDrawerOpen(open) {
    if (!drawer || !overlay || !toggleBtn) return;

    const shouldOpen = Boolean(open) && isMobile();
    drawer.classList.toggle("open", shouldOpen);
    overlay.classList.toggle("open", shouldOpen);
    drawer.setAttribute("aria-hidden", shouldOpen ? "false" : "true");
    toggleBtn.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
    document.body.classList.toggle("mobile-drawer-open", shouldOpen);
  }

  function scrollToTop() {
    dashboardMain?.scrollTo({ top: 0, behavior: "auto" });
    window.scrollTo({ top: 0, behavior: "auto" });
  }

  function renderMenu(visiblePanels) {
    if (!menu) return;

    menu.innerHTML = "";

    if (!visiblePanels.length) {
      const empty = document.createElement("div");
      empty.className = "mobile-panel-menu-empty";
      empty.textContent = "当前没有可显示的板块。";
      menu.appendChild(empty);
      return;
    }

    visiblePanels.forEach((panel) => {
      const { id, navTitle, fullTitle } = getPanelMeta(panel);
      const item = document.createElement("button");
      item.type = "button";
      item.className = "mobile-panel-menu-item";
      item.dataset.panel = id;
      item.classList.toggle("active", id === ctx.state.mobilePanel);

      const textWrap = document.createElement("span");
      textWrap.className = "mobile-panel-menu-text";

      const title = document.createElement("span");
      title.className = "mobile-panel-menu-title";
      title.textContent = navTitle;
      textWrap.appendChild(title);

      if (fullTitle && fullTitle !== navTitle) {
        const sub = document.createElement("span");
        sub.className = "mobile-panel-menu-subtitle";
        sub.textContent = fullTitle;
        textWrap.appendChild(sub);
      }

      const caret = document.createElement("i");
      caret.className = "ph ph-caret-right mobile-panel-menu-caret";

      item.append(textWrap, caret);
      menu.appendChild(item);
    });
  }

  function apply() {
    const visiblePanels = getVisiblePanels();
    const visibleIds = visiblePanels.map((panel) => getPanelId(panel));

    if (visibleIds.length && !visibleIds.includes(ctx.state.mobilePanel)) {
      ctx.state.mobilePanel = visibleIds[0];
      ctx.store.set(STORAGE_KEYS.mobilePanel, ctx.state.mobilePanel);
    }

    panels.forEach((panel) => {
      const id = getPanelId(panel);
      panel.classList.toggle("mobile-active", isMobile() && id === ctx.state.mobilePanel && !panel.hidden);
    });

    renderMenu(visiblePanels);

    if (!isMobile()) {
      setDrawerOpen(false);
      panels.forEach((panel) => panel.classList.remove("mobile-active"));
    }
  }

  function selectPanel(id) {
    const visibleIds = getVisiblePanels().map((panel) => getPanelId(panel));
    if (!visibleIds.includes(id)) return;

    ctx.state.mobilePanel = id;
    ctx.store.set(STORAGE_KEYS.mobilePanel, id);
    apply();
    setDrawerOpen(false);
    scrollToTop();
  }

  function bindMenuActions() {
    toggleBtn?.addEventListener("click", () => {
      setDrawerOpen(!drawer?.classList.contains("open"));
    });

    closeBtn?.addEventListener("click", () => {
      setDrawerOpen(false);
    });

    overlay?.addEventListener("click", () => {
      setDrawerOpen(false);
    });

    menu?.addEventListener("click", (event) => {
      const item = event.target.closest(".mobile-panel-menu-item[data-panel]");
      if (!item) return;
      selectPanel(item.dataset.panel);
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        setDrawerOpen(false);
      }
    });
  }

  function bindResize() {
    window.addEventListener("resize", apply);
  }

  function bindVisibilitySync() {
    document.addEventListener("ig:layout-visibility-changed", () => {
      apply();
    });
  }

  return {
    init() {
      bindMenuActions();
      bindResize();
      bindVisibilitySync();
      apply();
    },
  };
}
