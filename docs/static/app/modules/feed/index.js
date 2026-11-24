import { setError, setLoading } from "../../core/dom.js?v=46";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js?v=46";
import { renderFreshness } from "../../core/export-meta.js?v=63";
import { renderTelegram } from "./render.js?v=46";

export function createFeedModule(ctx) {
  const container = document.getElementById("telegram-container");

  const updatedAt = document.getElementById("telegramUpdatedAt");
  let snapshotAt = null;
  let selectedSource = "";
  const filter = document.createElement("select");
  filter.setAttribute("aria-label", "情报频道筛选");
  container?.before(filter);
  let fullRequest = null;
  let generation = 0;
  let preview = [];
  async function renderSelected() {
    const version = generation;
    if (selectedSource) {
      if (!fullRequest) {
        fullRequest = ctx.api.get("/api/telegram?limit=50").catch(error => {
          if (version === generation) fullRequest = null;
          throw error;
        });
      }
      const rows = await fullRequest;
      if (version !== generation) return;
      ctx.state.telegram = Array.isArray(rows) ? rows : [];
    }
    const rows = selectedSource ? ctx.state.telegram.filter(item => item.source_name === selectedSource) : preview;
    renderTelegram(ctx, container, rows.slice(0, 50));
  }
  filter.addEventListener("change", () => {
    selectedSource = filter.value;
    renderSelected().catch(error => {
      console.error("Failed to load channel", error);
      setError(container, "频道加载失败，请重新选择重试。");
    });
  });

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
      const version = ++generation;
      // Older publications may not have the preview yet; keep them usable.
      const data = await ctx.api.get("/api/telegram/preview").catch(() => ctx.api.get("/api/telegram?limit=50"));
      if (version !== generation) return;
      preview = Array.isArray(data) ? data.slice(0, 50) : data.items || [];
      ctx.state.telegram = preview;
      fullRequest = Array.isArray(data) ? Promise.resolve(data) : null;
      snapshotAt = data.snapshot_at || preview.map(item => item?.fetched_at).filter(Boolean).sort().at(-1);
      const sources = Array.isArray(data) ? [...new Set(data.map(item => item.source_name).filter(Boolean))] : data.sources || [];
      filter.replaceChildren();
      for (const name of ["", ...sources]) {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name || "全部频道";
        filter.appendChild(option);
      }
      if (![...filter.options].some(option => option.value === selectedSource)) selectedSource = "";
      filter.value = selectedSource;
      await renderSelected();
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
