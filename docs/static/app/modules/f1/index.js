import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { renderF1Live, getF1LiveStatusMarkup } from "./render-live.js";
import { renderF1News } from "./render-news.js";

function findSectionRoot() {
  const direct = document.querySelector(".col-f1");
  if (direct) return direct;

  const sections = Array.from(document.querySelectorAll(".grid-col"));
  for (const section of sections) {
    const h3 = section.querySelector("h3");
    if (h3 && /f1/i.test(h3.textContent || "")) {
      return section;
    }
  }
  return null;
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function ensureHost(section) {
  let host = section.querySelector(".f1-module-root");
  if (host) return host;

  host = document.createElement("div");
  host.className = "f1-module-root";
  host.style.minHeight = "0";
  host.style.display = "flex";
  host.style.flexDirection = "column";
  host.style.flex = "1";

  const header = section.querySelector(".section-header");
  if (header && header.parentNode === section) {
    header.insertAdjacentElement("afterend", host);
  } else {
    section.appendChild(host);
  }
  return host;
}

function buildShell() {
  return `
    <div class="f1-tabs">
      <button class="f1-tab-btn active" type="button" data-f1-tab-btn="live">实况</button>
      <button class="f1-tab-btn" type="button" data-f1-tab-btn="news">新闻</button>
    </div>

    <div class="f1-tab-panel active" data-f1-tab-panel="live">
      <div class="f1-panel-tools">
        <span class="f1-refresh-note" data-f1-live-status>加载中</span>
      </div>
      <div class="f1-panel-scroll" data-f1-live-panel>
        <div class="f1-empty">正在加载 F1 结果...</div>
      </div>
    </div>

    <div class="f1-tab-panel" data-f1-tab-panel="news">
      <div class="f1-panel-tools">
        <span class="f1-refresh-note" data-f1-news-status>加载中</span>
      </div>
      <div class="f1-panel-scroll" data-f1-news-panel>
        <div class="f1-empty">正在加载 F1 新闻...</div>
      </div>
    </div>
  `;
}

function setStatus(el, text, isError = false) {
  if (!el) return;
  el.textContent = text || "";
  el.classList.toggle("f1-refresh-error", !!isError);
}

function setStatusMarkup(el, markup, isError = false) {
  if (!el) return;
  el.innerHTML = markup || "";
  el.classList.toggle("f1-refresh-error", !!isError);
}

function setSimpleMessage(panelEl, message) {
  if (!panelEl) return;
  panelEl.innerHTML = `<div class="f1-empty">${esc(message)}</div>`;
}

function buildUpdatedLabel(value) {
  if (!value) return "暂无更新时间";
  return `已更新 ${formatRelativeLocalTime(value)}`;
}

function pickIsoCandidates(values) {
  return values
    .filter(Boolean)
    .map((value) => {
      const ts = new Date(value).getTime();
      return Number.isNaN(ts) ? null : { value: new Date(ts).toISOString(), ts };
    })
    .filter(Boolean)
    .sort((a, b) => b.ts - a.ts);
}

function resolveLiveUpdatedAt(payload, meta) {
  const candidates = pickIsoCandidates([
    payload?.updated_at,
    payload?.fetched_at,
    payload?.snapshot_fetched_at,
    payload?.generated_at,
    payload?.session?.updated_at,
    payload?.session?.fetched_at,
    meta?.lastModified,
  ]);
  return candidates[0]?.value || null;
}

function resolveNewsUpdatedAt(payload, meta) {
  const itemTimes = Array.isArray(payload)
    ? payload.flatMap((item) => [item?.fetched_at, item?.updated_at, item?.published_at])
    : [];

  const candidates = pickIsoCandidates([
    ...itemTimes,
    meta?.lastModified,
  ]);
  return candidates[0]?.value || null;
}

function applyNewsStatus(statusEl, updatedAt, isError = false) {
  if (!statusEl) return;
  setStatus(statusEl, buildUpdatedLabel(updatedAt), isError);
  statusEl.title = updatedAt ? formatAbsoluteLocalDateTime(updatedAt) : "";
}

function applyLiveStatus(statusEl, payload, updatedAt) {
  if (!statusEl) return;
  if (!payload || payload.error) {
    setStatusMarkup(statusEl, getF1LiveStatusMarkup(payload), true);
    statusEl.title = "";
    return;
  }

  const updatedTitle = updatedAt ? formatAbsoluteLocalDateTime(updatedAt) : "";
  setStatusMarkup(
    statusEl,
    getF1LiveStatusMarkup(payload, {
      updatedText: buildUpdatedLabel(updatedAt),
      updatedTitle,
    }),
    false,
  );
  statusEl.title = updatedTitle;
}

function wireTabs(host) {
  const buttons = Array.from(host.querySelectorAll("[data-f1-tab-btn]"));
  const panels = Array.from(host.querySelectorAll("[data-f1-tab-panel]"));

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = btn.getAttribute("data-f1-tab-btn");
      buttons.forEach((b) => b.classList.toggle("active", b === btn));
      panels.forEach((panel) => {
        panel.classList.toggle("active", panel.getAttribute("data-f1-tab-panel") === tab);
      });
    });
  });
}

export function createF1Module(ctx) {
  const section = findSectionRoot();

  function updateHeader() {
    if (!section) return;
    const host = ensureHost(section);
    const liveStatusEl = host.querySelector("[data-f1-live-status]");
    const newsStatusEl = host.querySelector("[data-f1-news-status]");

    if (ctx.state.f1LiveLoaded) {
      applyLiveStatus(liveStatusEl, ctx.state.f1Live, ctx.state.lastF1LiveFetchedAt || null);
    }
    if (ctx.state.f1NewsLoaded) {
      applyNewsStatus(newsStatusEl, ctx.state.lastF1NewsFetchedAt || null, false);
    }
  }

  async function refreshLive(host, doBackendRefresh = false) {
    const livePanel = host.querySelector("[data-f1-live-panel]");
    const statusEl = host.querySelector("[data-f1-live-status]");
    setStatus(statusEl, doBackendRefresh ? "刷新中..." : "加载中...");

    if (doBackendRefresh) {
      await ctx.api.post("/api/f1/refresh-live");
    }

    const loader = typeof ctx.api.getWithMeta === "function"
      ? ctx.api.getWithMeta("/api/f1/live")
      : Promise.resolve({ data: await ctx.api.get("/api/f1/live"), meta: {} });

    const { data: payload, meta } = await loader;
    const updatedAt = resolveLiveUpdatedAt(payload, meta);

    renderF1Live(livePanel, payload);
    ctx.state.lastF1LiveFetchedAt = updatedAt;
    ctx.state.lastF1FetchedAt = updatedAt;
    ctx.state.f1Live = payload;
    ctx.state.f1LiveLoaded = true;
    applyLiveStatus(statusEl, payload, updatedAt);
    return payload;
  }

  async function refreshNews(host, doBackendRefresh = false) {
    const newsPanel = host.querySelector("[data-f1-news-panel]");
    const statusEl = host.querySelector("[data-f1-news-status]");
    setStatus(statusEl, doBackendRefresh ? "刷新中..." : "加载中...");

    if (doBackendRefresh) {
      await ctx.api.post("/api/f1/refresh-news");
    }

    const loader = typeof ctx.api.getWithMeta === "function"
      ? ctx.api.getWithMeta("/api/f1/news?limit=20")
      : Promise.resolve({ data: await ctx.api.get("/api/f1/news?limit=20"), meta: {} });

    const { data: payload, meta } = await loader;
    const updatedAt = resolveNewsUpdatedAt(payload, meta);

    renderF1News(ctx, newsPanel, payload);
    ctx.state.lastF1NewsFetchedAt = updatedAt;
    ctx.state.f1News = Array.isArray(payload) ? payload : [];
    ctx.state.f1NewsLoaded = true;
    applyNewsStatus(statusEl, updatedAt, false);
    return payload;
  }

  return {
    async refresh() {
      if (!section) return;
      const host = ensureHost(section);
      await refreshLive(host, false);
      await refreshNews(host, false);
    },

    updateHeader,

    init() {
      if (!section) {
        console.warn("F1 section root not found; skipping F1 module init.");
        return;
      }

      const host = ensureHost(section);
      host.innerHTML = buildShell();
      wireTabs(host);

      refreshLive(host, false).catch((err) => {
        console.error("F1 live load failed:", err);
        setSimpleMessage(host.querySelector("[data-f1-live-panel]"), `F1 实况加载失败：${err?.message || err}`);
        setStatus(host.querySelector("[data-f1-live-status]"), "加载失败", true);
      });

      refreshNews(host, false).catch((err) => {
        console.error("F1 news load failed:", err);
        setSimpleMessage(host.querySelector("[data-f1-news-panel]"), `F1 新闻加载失败：${err?.message || err}`);
        setStatus(host.querySelector("[data-f1-news-status]"), "加载失败", true);
      });
    },
  };
}

export default createF1Module;
