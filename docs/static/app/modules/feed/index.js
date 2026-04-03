import { setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { renderTelegram } from "./render.js";

export function createFeedModule(ctx) {
  const container = document.getElementById("telegram-container");
  const refreshBtn = document.getElementById("feedRefreshBtn");

  function updateRelativeTimes() {
    document.querySelectorAll(".feed-type[data-time-value]").forEach((el) => {
      const value = el.dataset.timeValue || "";
      el.textContent = formatRelativeLocalTime(value);
      el.title = formatAbsoluteLocalDateTime(value);
    });
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";

    try {
      await ctx.api.post("/api/telegram/refresh");
      await refresh();
    } catch (error) {
      console.error("Manual telegram refresh failed:", error);
      alert("Telegram 手动刷新失败。");
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      if (label) label.textContent = "刷新";
    }
  }

  async function refresh() {
    setLoading(container, "正在加载情报...");

    try {
      const data = await ctx.api.get("/api/telegram?limit=50");
      ctx.state.telegram = Array.isArray(data) ? data : [];
      renderTelegram(ctx, container, ctx.state.telegram);
    } catch (error) {
      console.error("Failed to load telegram data:", error);
      setError(container, "情报加载失败。");
    }
  }

  return {
    refresh,
    updateRelativeTimes,
    init() {
      refreshBtn?.addEventListener("click", manualRefresh);
    },
  };
}
