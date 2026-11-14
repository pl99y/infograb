import { setError, setLoading } from "../../core/dom.js?v=46";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js?v=46";
import { renderFreshness } from "../../core/export-meta.js?v=46";
import { renderTelegram } from "./render.js?v=46";

export function createFeedModule(ctx) {
  const container = document.getElementById("telegram-container");

  const updatedAt = document.getElementById("telegramUpdatedAt");
  let snapshotAt = null;
  let selectedSource = "";
  const filter = document.createElement("select");
  filter.setAttribute("aria-label", "情报频道筛选");
  container?.before(filter);
  function renderSelected() {
    renderTelegram(ctx, container, ctx.state.telegram.filter(item => !selectedSource || item.source_name === selectedSource));
  }
  filter.addEventListener("change", () => { selectedSource = filter.value; renderSelected(); });

  function updateRelativeTimes() {
    renderFreshness(updatedAt, ["telegram.json"], snapshotAt);
    document.querySelectorAll(".feed-type[data-time-value]").forEach((el) => {
      const value = el.dataset.timeValue || "";
      el.textContent = formatRelativeLocalTime(value);
      el.title = formatAbsoluteLocalDateTime(value);
    });
  }

  async function refresh() {
    setLoading(container, "正在加载情报...");

    try {
      const data = await ctx.api.get("/api/telegram?limit=50");
      ctx.state.telegram = Array.isArray(data) ? data : [];
      snapshotAt = ctx.state.telegram.map(item => item?.fetched_at).filter(Boolean).sort().at(-1);
      filter.replaceChildren();
      for (const name of ["", ...new Set(ctx.state.telegram.map(item => item.source_name).filter(Boolean))]) {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name || "全部频道 · 各频道保留最近 3 条，其余按时间";
        filter.appendChild(option);
      }
      if (![...filter.options].some(option => option.value === selectedSource)) selectedSource = "";
      filter.value = selectedSource;
      renderSelected();
    } catch (error) {
      console.error("Failed to load telegram data:", error);
      setError(container, "情报加载失败。");
    }
  }

  return {
    refresh,
    updateRelativeTimes,
    init() {
    },
  };
}
