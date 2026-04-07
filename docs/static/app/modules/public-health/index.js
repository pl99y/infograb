import { setEmpty, setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function pickLatestTimestamp(payload) {
  const values = [];
  if (payload?.updated_at) values.push(new Date(payload.updated_at).getTime());

  for (const list of [payload?.early_warning, payload?.outbreak_events]) {
    for (const item of Array.isArray(list) ? list : []) {
      if (item?.published_at) values.push(new Date(item.published_at).getTime());
      if (item?.fetched_at) values.push(new Date(item.fetched_at).getTime());
    }
  }

  const valid = values.filter((value) => !Number.isNaN(value));
  if (!valid.length) return null;
  return new Date(Math.max(...valid)).toISOString();
}

function normalizeItem(item) {
  return {
    id: item?.id ?? null,
    sourceKey: item?.source_key || "",
    sourceName: item?.source_name || "",
    categoryKey: item?.category_key || "",
    title: item?.title || item?.title_zh || item?.title_raw || "",
    titleRaw: item?.title_raw || "",
    dateRaw: item?.date_raw || "",
    publishedAt: item?.published_at || null,
    itemUrl: item?.item_url || "",
    listUrl: item?.list_url || "",
    fetchedAt: item?.fetched_at || null,
  };
}

const WHO_OUTBREAK_LIST_URL = "https://www.who.int/emergencies/disease-outbreak-news";

function resolveItemHref(item) {
  if (item?.categoryKey === "outbreak_event") {
    return WHO_OUTBREAK_LIST_URL;
  }
  return item?.itemUrl || item?.listUrl || "";
}

function formatCardTime(item) {
  if (item?.publishedAt) {
    return formatAbsoluteLocalDateTime(item.publishedAt);
  }
  return item?.dateRaw || "—";
}

function renderList(items, emptyText) {
  if (!Array.isArray(items) || !items.length) {
    return `
      <div class="public-health-empty">
        <div class="public-health-empty-title">暂无数据</div>
        <div class="public-health-empty-subtitle">${escapeHtml(emptyText)}</div>
      </div>
    `;
  }

  return items.map((item) => {
    const href = resolveItemHref(item);
    const sourceClass = item.sourceKey === "promed" ? "promed" : "who";
    const relative = item.publishedAt ? formatRelativeLocalTime(item.publishedAt) : (item.dateRaw || "");
    return `
      <article class="public-health-card ${href ? "has-link" : ""}" ${href ? `data-card-url="${escapeHtml(href)}" tabindex="0"` : ""}>
        <div class="public-health-card-top">
          <span class="public-health-source-badge ${sourceClass}">${escapeHtml(item.sourceName)}</span>
          <span class="public-health-time" title="${escapeHtml(formatCardTime(item))}">${escapeHtml(relative)}</span>
        </div>
        <div class="public-health-title">${escapeHtml(item.title)}</div>
        <div class="public-health-meta-row">
          <span class="public-health-date">${escapeHtml(item.dateRaw || "")}</span>
          ${href ? `<a class="public-health-link" href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">打开来源</a>` : ""}
        </div>
      </article>
    `;
  }).join("");
}

export function createPublicHealthModule(ctx) {
  const tabs = document.getElementById("publicHealthTabs");
  const updatedAt = document.getElementById("publicHealthUpdatedAt");
  const refreshBtn = document.getElementById("publicHealthRefreshBtn");
  const earlyPanel = document.getElementById("publicHealthEarlyPanel");
  const outbreakPanel = document.getElementById("publicHealthOutbreakPanel");
  const earlyContainer = document.getElementById("publicHealthEarlyContainer");
  const outbreakContainer = document.getElementById("publicHealthOutbreakContainer");

  let activeTab = "early-warning";

  function updateHeader() {
    if (!updatedAt) return;
    const value = ctx.state.lastPublicHealthFetchedAt;
    if (!value) {
      updatedAt.textContent = "（加载中）";
      updatedAt.title = "";
      return;
    }
    updatedAt.textContent = `（${formatRelativeLocalTime(value)}更新）`;
    updatedAt.title = formatAbsoluteLocalDateTime(value);
  }

  function applyTab() {
    const buttons = Array.from(tabs?.querySelectorAll(".public-health-tab-btn") || []);
    buttons.forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === activeTab));
    earlyPanel?.classList.toggle("active", activeTab === "early-warning");
    outbreakPanel?.classList.toggle("active", activeTab === "outbreak-event");
  }

  function render() {
    if (earlyContainer) {
      earlyContainer.innerHTML = renderList(ctx.state.publicHealthEarlyWarnings, "还没有抓到 ProMED 早期预警。");
    }
    if (outbreakContainer) {
      outbreakContainer.innerHTML = renderList(ctx.state.publicHealthOutbreakEvents, "还没有抓到 WHO 爆发事件。");
    }
  }

  async function refresh() {
    if (earlyContainer) setLoading(earlyContainer, "正在加载早期预警...");
    if (outbreakContainer) setLoading(outbreakContainer, "正在加载爆发事件...");

    try {
      const payload = await ctx.api.get("/api/public-health/latest?limit_early_warning=120&limit_outbreak_events=120");
      ctx.state.publicHealthEarlyWarnings = (payload?.early_warning || []).map(normalizeItem);
      ctx.state.publicHealthOutbreakEvents = (payload?.outbreak_events || []).map(normalizeItem);
      ctx.state.lastPublicHealthFetchedAt = pickLatestTimestamp(payload);
      render();
      updateHeader();
    } catch (error) {
      console.error("Failed to load public health module:", error);
      ctx.state.lastPublicHealthFetchedAt = null;
      updateHeader();
      if (earlyContainer) setError(earlyContainer, "公众健康数据加载失败。请检查 /api/public-health/latest。");
      if (outbreakContainer) setError(outbreakContainer, "公众健康数据加载失败。请检查 /api/public-health/latest。");
    }
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";
    try {
      await ctx.api.post("/api/public-health/refresh");
      await refresh();
    } catch (error) {
      console.error("Manual public health refresh failed:", error);
      alert("公众健康模块手动刷新失败。请检查 /api/public-health/refresh。");
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      if (label) label.textContent = "刷新";
    }
  }

  function bindCardLinks(host) {
    host?.addEventListener("click", (event) => {
      const card = event.target.closest(".public-health-card[data-card-url]");
      if (!card) return;
      if (event.target.closest("a, button, input, textarea, select")) return;
      const href = card.dataset.cardUrl;
      if (!href) return;
      window.open(href, "_blank", "noopener,noreferrer");
    });

    host?.addEventListener("keydown", (event) => {
      const card = event.target.closest(".public-health-card[data-card-url]");
      if (!card) return;
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      const href = card.dataset.cardUrl;
      if (!href) return;
      window.open(href, "_blank", "noopener,noreferrer");
    });
  }

  return {
    refresh,
    updateHeader,
    init() {
      tabs?.addEventListener("click", (event) => {
        const button = event.target.closest(".public-health-tab-btn");
        if (!button) return;
        const next = button.dataset.tab;
        if (!next) return;
        activeTab = next;
        applyTab();
      });
      refreshBtn?.addEventListener("click", manualRefresh);
      bindCardLinks(earlyContainer);
      bindCardLinks(outbreakContainer);
      applyTab();
      render();
      updateHeader();
    },
  };
}

export default createPublicHealthModule;
