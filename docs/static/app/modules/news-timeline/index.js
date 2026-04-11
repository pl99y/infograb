import { setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { getExportProfileGeneratedAt } from "../../core/export-meta.js";
import { renderNewsTimeline } from "./render.js";

export function createNewsTimelineModule(ctx) {
  const container = document.getElementById("newsTimelineContainer");
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
      const exportGeneratedAt = await getExportProfileGeneratedAt("15m");
      ctx.state.lastNewsTimelineFetchedAt = exportGeneratedAt || data?.updated_at || null;
      renderNewsTimeline(ctx, container, ctx.state.newsTimeline);
      updateHeader();
    } catch (error) {
      console.error("Failed to load news timeline:", error);
      setError(container, "新闻流加载失败。");
      updateHeader();
    }
  }

  return {
    refresh,
    updateHeader,
    updateRelativeTimes,
    init() {
      updateHeader();
    },
  };
}
