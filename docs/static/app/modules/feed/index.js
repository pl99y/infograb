import { setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { renderFreshness } from "../../core/export-meta.js?v=44";
import { renderTelegram } from "./render.js";

export function createFeedModule(ctx) {
  const container = document.getElementById("telegram-container");

  const updatedAt = document.getElementById("telegramUpdatedAt");
  let snapshotAt = null;

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
    },
  };
}
