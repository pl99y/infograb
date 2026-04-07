import { AUTO_REFRESH_MS, RELATIVE_CLOCK_MS, STORAGE_KEYS } from "./core/constants.js";
import { createApi } from "./core/api.js";
import { createStore } from "./core/store.js";
import { createThemeModule } from "./modules/theme/index.js";
import { createMobileModule } from "./modules/mobile/index.js";
import { createLightboxModule } from "./modules/lightbox/index.js";
import { createLayoutModule } from "./modules/layout/index.js";
import { createEnergyModule } from "./modules/energy/index.js";
import { createMarketModule } from "./modules/market/index.js";
import { createIflowModule } from "./modules/iflow/index.js";
import { createAviationModule } from "./modules/aviation/index.js";
import { createDisasterModule } from "./modules/disaster/index.js";
import { createPublicHealthModule } from "./modules/public-health/index.js";
import { createSpaceWeatherModule } from "./modules/space-weather/index.js";
import { createMndPlaModule } from "./modules/mnd-pla/index.js";
import { createF1Module } from "./modules/f1/index.js";
import { createFeedModule } from "./modules/feed/index.js";

function createContext() {
  const store = createStore();
  return {
    api: createApi(),
    store,
    state: {
      theme: store.get(STORAGE_KEYS.theme, "dark"),
      mobilePanel: store.get(STORAGE_KEYS.mobilePanel, "feed"),
      aviationTab: store.get(STORAGE_KEYS.aviationTab, "alerts"),
      layoutEditMode: false,
      energy: [],
      marketSnapshots: [],
      iflowData: null,
      aviationAlerts: [],
      aviationDisruptions: [],
      disasterInstant: [],
      publicHealthEarlyWarnings: [],
      publicHealthOutbreakEvents: [],
      telegram: [],
      spaceWeatherAlerts: [],
      spaceWeatherForecast: null,
      mndPlaData: null,
      f1Live: null,
      f1News: [],
      translationCache: new Map(),
      lastEnergyFetchedAt: null,
      lastMarketFetchedAt: null,
      lastIflowFetchedAt: null,
      lastAviationFetchedAt: null,
      lastDisasterFetchedAt: null,
      lastPublicHealthFetchedAt: null,
      lastSpaceWeatherFetchedAt: null,
      lastMndPlaFetchedAt: null,
      lastF1FetchedAt: null,
      refreshTimer: null,
      relativeClockTimer: null,
    },
  };
}

function safeInit(label, factory, ctx, modules) {
  try {
    const mod = factory(ctx);
    mod?.init?.();
    if (mod) modules.push(mod);
    return mod;
  } catch (error) {
    console.error(`[init failed] ${label}`, error);
    return null;
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  const ctx = createContext();
  const modules = [];

  document.body.classList.remove("edit-layout-mode", "layout-dragging");

  ctx.lightbox = safeInit("lightbox", createLightboxModule, ctx, modules);
  ctx.theme = safeInit("theme", createThemeModule, ctx, modules);
  ctx.f1Module = safeInit("f1", createF1Module, ctx, modules);
  ctx.layout = safeInit("layout", createLayoutModule, ctx, modules);
  ctx.mobile = safeInit("mobile", createMobileModule, ctx, modules);
  ctx.energyModule = safeInit("energy", createEnergyModule, ctx, modules);
  ctx.marketModule = safeInit("market", createMarketModule, ctx, modules);
  ctx.spaceWeatherModule = safeInit("space-weather", createSpaceWeatherModule, ctx, modules);
  ctx.mndPlaModule = safeInit("mnd-pla", createMndPlaModule, ctx, modules);
  ctx.iflowModule = safeInit("iflow", createIflowModule, ctx, modules);
  ctx.aviationModule = safeInit("aviation", createAviationModule, ctx, modules);
  ctx.disasterModule = safeInit("disaster", createDisasterModule, ctx, modules);
  ctx.publicHealthModule = safeInit("public-health", createPublicHealthModule, ctx, modules);
  ctx.feedModule = safeInit("feed", createFeedModule, ctx, modules);

  async function refreshAll() {
    const refreshers = modules.filter((mod) => typeof mod.refresh === "function").map((mod) => mod.refresh());
    await Promise.all(refreshers);
  }

  function updateRelativeTexts() {
    try { ctx.energyModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] energy", error); }
    try { ctx.marketModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] market", error); }
    try { ctx.spaceWeatherModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] space-weather", error); }
    try { ctx.mndPlaModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] mnd-pla", error); }
    try { ctx.iflowModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] iflow", error); }
    try { ctx.aviationModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] aviation", error); }
    try { ctx.disasterModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] disaster", error); }
    try { ctx.publicHealthModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] public-health", error); }
    try { ctx.f1Module?.updateHeader?.(); } catch (error) { console.error("[relative update failed] f1", error); }
    try { ctx.feedModule?.updateRelativeTimes?.(); } catch (error) { console.error("[relative update failed] feed", error); }
  }

  try {
    await refreshAll();
    updateRelativeTexts();
  } catch (error) {
    console.error("Initial load failed:", error);
  }

  ctx.state.refreshTimer = setInterval(() => {
    refreshAll().catch((error) => console.error("Auto refresh failed:", error));
  }, AUTO_REFRESH_MS);

  ctx.state.relativeClockTimer = setInterval(() => {
    updateRelativeTexts();
  }, RELATIVE_CLOCK_MS);
});
