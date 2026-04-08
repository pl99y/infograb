import { setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { renderNewsTimeline } from "./render.js";

export function createNewsTimelineModule(ctx) {
  const container = document.getElementById("newsTimelineContainer");
  const refreshBtn = document.getElementById("newsTimelineRefreshBtn");
  const updatedEl = document.getElementById("newsTimelineUpdatedAt");

  function updateHeader() {
    if (!updatedEl) return;
    const value = ctx.state.lastNewsTimelineFetchedAt;
    if (!value) {
      updatedEl.textContent = "（加载中）";
      return;
    }
    updatedEl.textContent = `（${formatRelativeLocalTime(value)}更新）`;
    updatedEl.title = formatAbsoluteLocalDateTime(value);
  }

  function updateRelativeTimes() {
    document.querySelectorAll("[data-news-time-value]").forEach((el) => {
      const value = el.dataset.newsTimeValue || "";
      el.textContent = formatRelativeLocalTime(value);
      el.title = formatAbsoluteLocalDateTime(value);
    });
  }

  async function refresh() {
    setLoading(container, "正在加载新闻流...");
    try {
      const data = await ctx.api.get("/api/news-timeline/latest?limit=120&window_hours=12");
      ctx.state.newsTimeline = Array.isArray(data?.items) ? data.items : [];
      ctx.state.lastNewsTimelineFetchedAt = data?.updated_at || null;
      renderNewsTimeline(ctx, container, ctx.state.newsTimeline);
      updateHeader();
    } catch (error) {
      console.error("Failed to load news timeline:", error);
      setError(container, "新闻流加载失败。");
      updateHeader();
    }
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";

    try {
      await ctx.api.post("/api/news-timeline/refresh");
      await refresh();
    } catch (error) {
      console.error("Manual news timeline refresh failed:", error);
      alert("新闻流手动刷新失败。");
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      if (label) label.textContent = "刷新";
    }
  }

  return {
    refresh,
    updateHeader,
    updateRelativeTimes,
    init() {
      updateHeader();
      refreshBtn?.addEventListener("click", manualRefresh);
    },
  };
}
