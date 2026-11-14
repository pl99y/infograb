import { AUTO_REFRESH_MS, RELATIVE_CLOCK_MS, STORAGE_KEYS } from "./core/constants.js?v=46";
import { createApi } from "./core/api.js?v=46";
import { refreshExportMeta } from "./core/export-meta.js?v=46";
import { createStore } from "./core/store.js?v=46";
import { createThemeModule } from "./modules/theme/index.js?v=46";
import { createMobileModule } from "./modules/mobile/index.js?v=46";
import { createLightboxModule } from "./modules/lightbox/index.js?v=46";
import { createLayoutModule } from "./modules/layout/index.js?v=46";
import { createEnergyModule } from "./modules/energy/index.js?v=47";
import { createMarketModule } from "./modules/market/index.js?v=46";
import { createAviationModule } from "./modules/aviation/index.js?v=46";
import { createDisasterModule } from "./modules/disaster/index.js?v=46";
import { createPublicHealthModule } from "./modules/public-health/index.js?v=46";
import { createSpaceWeatherModule } from "./modules/space-weather/index.js?v=46";
import { createMndPlaModule } from "./modules/mnd-pla/index.js?v=46";
import { createF1Module } from "./modules/f1/index.js?v=46";
import { createFeedModule } from "./modules/feed/index.js?v=46";
import { createNewsTimelineModule } from "./modules/news-timeline/index.js?v=46";

function createContext() {
  const store = createStore();
  return {
    api: createApi(),
    store,
    state: {
      theme: store.get(STORAGE_KEYS.theme, "dark"),
      mobilePanel: store.get(STORAGE_KEYS.mobilePanel, "news-timeline"),
      aviationTab: store.get(STORAGE_KEYS.aviationTab, "alerts"),
      layoutEditMode: false,
      energy: [],
      marketSnapshots: [],
      aviationAlerts: [],
      aviationDisruptions: [],
      disasterInstant: [],
      publicHealthEarlyWarnings: [],
      publicHealthOutbreakEvents: [],
      telegram: [],
      newsTimeline: [],
      spaceWeatherLaunches: null,
      spaceLaunches: null,
      spaceWeatherAlerts: [],
      spaceWeatherForecast: null,
      mndPlaData: null,
      f1Live: null,
      f1News: [],
      translationCache: new Map(),
      lastEnergyFetchedAt: null,
      lastMarketFetchedAt: null,
      lastAviationFetchedAt: null,
      lastDisasterFetchedAt: null,
      lastPublicHealthFetchedAt: null,
      lastSpaceWeatherFetchedAt: null,
      lastMndPlaFetchedAt: null,
      lastNewsTimelineFetchedAt: null,
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
  ctx.aviationModule = safeInit("aviation", createAviationModule, ctx, modules);
  ctx.disasterModule = safeInit("disaster", createDisasterModule, ctx, modules);
  ctx.publicHealthModule = safeInit("public-health", createPublicHealthModule, ctx, modules);
  ctx.newsTimelineModule = safeInit("news-timeline", createNewsTimelineModule, ctx, modules);
  ctx.feedModule = safeInit("feed", createFeedModule, ctx, modules);

  let refreshing = false;

  async function refreshAll() {
    if (refreshing) return;
    refreshing = true;
    try {
      const tasks = [refreshExportMeta()];
      for (const mod of modules) {
        if (typeof mod.refresh === "function") tasks.push(Promise.resolve().then(() => mod.refresh()));
      }
      const results = await Promise.allSettled(tasks);
      for (const result of results) {
        if (result.status === "rejected") console.error("Dashboard refresh failed:", result.reason);
      }
      updateRelativeTexts();
    } finally {
      refreshing = false;
    }
  }

  function updateRelativeTexts() {
    try { ctx.energyModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] energy", error); }
    try { ctx.marketModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] market", error); }
    try { ctx.spaceWeatherModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] space-weather", error); }
    try { ctx.mndPlaModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] mnd-pla", error); }
    try { ctx.aviationModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] aviation", error); }
    try { ctx.disasterModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] disaster", error); }
    try { ctx.publicHealthModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] public-health", error); }
    try { ctx.newsTimelineModule?.updateRelativeTimes?.(); ctx.newsTimelineModule?.updateHeader?.(); } catch (error) { console.error("[relative update failed] news-timeline", error); }
    try { ctx.f1Module?.updateHeader?.(); } catch (error) { console.error("[relative update failed] f1", error); }
    try { ctx.feedModule?.updateRelativeTimes?.(); } catch (error) { console.error("[relative update failed] feed", error); }
  }

  try {
    await refreshAll();
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
