import { setError, setLoading } from "../../core/dom.js";
import { STORAGE_KEYS } from "../../core/constants.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { getExportProfileGeneratedAt } from "../../core/export-meta.js";
import { renderAviationAlerts } from "./render-alerts.js";
import { renderAviationDisruptions } from "./render-disruptions.js";

export function createAviationModule(ctx) {
  const alertsContainer = document.getElementById("aviation-alerts-container");
  const disruptionsContainer = document.getElementById("aviation-disruptions-container");
  const updatedAt = document.getElementById("aviationUpdatedAt");
  const refreshBtn = document.getElementById("aviationRefreshBtn");
  const tabs = document.getElementById("aviationTabs");
  const alertsPanel = document.getElementById("aviationAlertsPanel");
  const disruptionsPanel = document.getElementById("aviationDisruptionsPanel");

  function applyTab() {
    tabs?.querySelectorAll(".aviation-tab-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === ctx.state.aviationTab);
    });

    alertsPanel?.classList.toggle("active", ctx.state.aviationTab === "alerts");
    disruptionsPanel?.classList.toggle("active", ctx.state.aviationTab === "disruptions");
    ctx.store.set(STORAGE_KEYS.aviationTab, ctx.state.aviationTab);
  }

  function updateHeader() {
    if (!updatedAt) return;
    if (!ctx.state.lastAviationFetchedAt) {
      updatedAt.textContent = "（暂无更新时间）";
      updatedAt.title = "";
      return;
    }

    updatedAt.textContent = `（${formatRelativeLocalTime(ctx.state.lastAviationFetchedAt)}更新）`;
    updatedAt.title = formatAbsoluteLocalDateTime(ctx.state.lastAviationFetchedAt);
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";

    try {
      await ctx.api.post("/api/aviation/refresh");
      await refresh();
    } catch (error) {
      console.error("Manual aviation refresh failed:", error);
      alert("航空模块手动刷新失败。");
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      if (label) label.textContent = "刷新";
    }
  }

  async function refresh() {
    setLoading(alertsContainer, "正在加载异常航班...");
    setLoading(disruptionsContainer, "正在加载机场扰动...");

    try {
      const [alerts, disruptions] = await Promise.all([
        ctx.api.get("/api/aviation/alerts?limit=30"),
        ctx.api.get("/api/aviation/disruptions?region=worldwide&direction=departures&limit=20"),
      ]);

      ctx.state.aviationAlerts = Array.isArray(alerts) ? alerts : [];
      ctx.state.aviationDisruptions = Array.isArray(disruptions) ? disruptions : [];

      renderAviationAlerts(alertsContainer, ctx.state.aviationAlerts);
      renderAviationDisruptions(disruptionsContainer, ctx.state.aviationDisruptions);

      const timestamps = [...ctx.state.aviationAlerts, ...ctx.state.aviationDisruptions]
        .map((item) => item.fetched_at)
        .filter(Boolean)
        .map((value) => new Date(value).getTime())
        .filter((value) => !Number.isNaN(value));

      const exportGeneratedAt = await getExportProfileGeneratedAt("15m");
      const fallbackUpdatedAt = timestamps.length > 0 ? new Date(Math.max(...timestamps)).toISOString() : null;
      ctx.state.lastAviationFetchedAt = exportGeneratedAt || fallbackUpdatedAt;
      updateHeader();
    } catch (error) {
      console.error("Failed to load aviation data:", error);
      ctx.state.lastAviationFetchedAt = null;
      updateHeader();
      setError(alertsContainer, "异常航班加载失败。");
      setError(disruptionsContainer, "机场扰动加载失败。");
    }
  }

  return {
    refresh,
    updateHeader,
    init() {
      tabs?.addEventListener("click", (event) => {
        const button = event.target.closest(".aviation-tab-btn");
        if (!button) return;
        const nextTab = button.dataset.tab;
        if (!nextTab) return;
        ctx.state.aviationTab = nextTab;
        applyTab();
      });

      refreshBtn?.addEventListener("click", manualRefresh);
      applyTab();
    },
  };
}
