import { setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { getExportProfileGeneratedAt } from "../../core/export-meta.js";
import { renderHotsearch, renderNewsTimeline } from "./render.js";

export function createNewsTimelineModule(ctx) {
  const container = document.getElementById("newsTimelineContainer");
  const hotsearchContainer = document.getElementById("newsHotsearchContainer");
  const updatedEl = document.getElementById("newsTimelineUpdatedAt");
  const tabsRoot = document.getElementById("newsPanelTabs");

  function updateHeader() {
    if (!updatedEl) return;
    const value = ctx.state.lastNewsTimelineFetchedAt || ctx.state.lastHotsearchFetchedAt;
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
    updateHeader();
  }

  function setActiveTab(tabName) {
    const target = tabName === "hotsearch" ? "hotsearch" : "timeline";
    document.querySelectorAll("[data-news-panel-tab]").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.newsPanelTab === target);
    });
    document.querySelectorAll("[data-news-panel]").forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.newsPanel === target);
    });
    try {
      ctx.store?.set?.("infograb.newsPanelTab", target);
    } catch (_) {}
  }

  async function refreshTimeline() {
    setLoading(container, "正在加载新闻流...");
    const data = await ctx.api.get("/api/news-timeline/latest?limit=120&window_hours=12");
    ctx.state.newsTimeline = Array.isArray(data?.items) ? data.items : [];
    renderNewsTimeline(ctx, container, ctx.state.newsTimeline);
    return data;
  }

  async function refreshHotsearch() {
    if (!hotsearchContainer) return null;
    setLoading(hotsearchContainer, "正在加载热搜观察...");
    const data = await ctx.api.get("/api/news-hotsearch/latest");
    ctx.state.hotsearch = data || null;
    ctx.state.lastHotsearchFetchedAt = data?.generated_at || null;
    renderHotsearch(ctx, hotsearchContainer, data);
    return data;
  }

  async function refresh() {
    try {
      const [timelineData] = await Promise.all([
        refreshTimeline(),
        refreshHotsearch().catch((error) => {
          console.error("Failed to load hotsearch:", error);
          setError(hotsearchContainer, "热搜观察加载失败。");
          return null;
        }),
      ]);

      const exportGeneratedAt = await getExportProfileGeneratedAt("15m");
      ctx.state.lastNewsTimelineFetchedAt = exportGeneratedAt || timelineData?.updated_at || null;
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
      const savedTab = ctx.store?.get?.("infograb.newsPanelTab", "timeline") || "timeline";
      setActiveTab(savedTab);
      tabsRoot?.addEventListener("click", (event) => {
        const btn = event.target?.closest?.("[data-news-panel-tab]");
        if (!btn) return;
        setActiveTab(btn.dataset.newsPanelTab || "timeline");
      });
      updateHeader();
    },
  };
}
