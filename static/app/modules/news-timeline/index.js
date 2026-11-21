import { setError, setLoading } from "../../core/dom.js?v=46";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js?v=46";
import { renderFreshness } from "../../core/export-meta.js?v=46";
import { renderHotsearch, renderNewsTimeline } from "./render.js?v=46";

export function createNewsTimelineModule(ctx) {
  const container = document.getElementById("newsTimelineContainer");
  const hotsearchContainer = document.getElementById("newsHotsearchContainer");
  const updatedEl = document.getElementById("newsTimelineUpdatedAt");
  const tabsRoot = document.getElementById("newsPanelTabs");

  let activeTab = "timeline";

  function updateHeader() {
    if (activeTab === "hotsearch") {
      renderFreshness(updatedEl, ["news_hotsearch_latest.json"], ctx.state.lastHotsearchFetchedAt);
    } else {
      renderFreshness(updatedEl, ["news_timeline_latest.json"], ctx.state.lastNewsTimelineFetchedAt);
    }
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
    activeTab = target;
    updateHeader();
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
    const data = await ctx.api.get("/api/news-timeline/latest?limit=-1&window_hours=12");
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

      ctx.state.lastNewsTimelineFetchedAt = timelineData?.updated_at || null;
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
