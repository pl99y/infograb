import { STORAGE_KEYS } from "../../core/constants.js";

export function createThemeModule(ctx) {
  const button = document.getElementById("themeToggleBtn");

  function apply() {
    document.body.classList.toggle("dark-theme", ctx.state.theme === "dark");
    const icon = button?.querySelector("i");
    if (icon) {
      icon.className = ctx.state.theme === "dark" ? "ph ph-sun" : "ph ph-moon";
    }
    ctx.store.set(STORAGE_KEYS.theme, ctx.state.theme);
  }

  return {
    init() {
      apply();
      button?.addEventListener("click", () => {
        ctx.state.theme = ctx.state.theme === "dark" ? "light" : "dark";
        apply();
      });
    },
  };
}
