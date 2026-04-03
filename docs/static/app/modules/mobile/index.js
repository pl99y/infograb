import { MOBILE_BREAKPOINT, STORAGE_KEYS } from "../../core/constants.js";

export function createMobileModule(ctx) {
  const buttons = Array.from(document.querySelectorAll(".mobile-tab-btn[data-panel]"));
  const panels = Array.from(document.querySelectorAll(".grid-stack-item[gs-id]"));

  function isMobile() {
    return window.innerWidth <= MOBILE_BREAKPOINT;
  }

  function getHiddenSet() {
    const raw = ctx.store.getJSON(STORAGE_KEYS.layout, {}) || {};
    const hidden = Array.isArray(raw.hidden) ? raw.hidden : [];
    return new Set(hidden);
  }

  function getVisiblePanelIds() {
    const hidden = getHiddenSet();
    return buttons
      .map((btn) => btn.dataset.panel)
      .filter((id) => !hidden.has(id));
  }

  function apply() {
    const visibleIds = getVisiblePanelIds();

    buttons.forEach((btn) => {
      const id = btn.dataset.panel;
      btn.hidden = !visibleIds.includes(id);
    });

    if (!visibleIds.length) return;

    if (!visibleIds.includes(ctx.state.mobilePanel)) {
      ctx.state.mobilePanel = visibleIds[0];
      ctx.store.set(STORAGE_KEYS.mobilePanel, ctx.state.mobilePanel);
    }

    buttons.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.panel === ctx.state.mobilePanel);
    });

    panels.forEach((panel) => {
      const id = panel.getAttribute("gs-id");
      panel.classList.toggle("mobile-active", id === ctx.state.mobilePanel);
    });

    if (!isMobile()) {
      panels.forEach((panel) => panel.classList.remove("mobile-active"));
    }
  }

  function bindButtons() {
    buttons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const id = btn.dataset.panel;
        if (btn.hidden) return;

        ctx.state.mobilePanel = id;
        ctx.store.set(STORAGE_KEYS.mobilePanel, id);
        apply();
      });
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
      bindButtons();
      bindResize();
      bindVisibilitySync();
      apply();
    },
  };
}