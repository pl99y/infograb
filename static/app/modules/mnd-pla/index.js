import { clearElement, setEmpty, setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { getExportProfileGeneratedAt } from "../../core/export-meta.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatInt(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return `${Number(value)}`;
}

function toArray(value) {
  if (Array.isArray(value)) return value;

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return [];

    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) return parsed;
    } catch (_) {
      // ignore
    }

    return trimmed
      .split(/[、,/]/)
      .map((x) => x.trim())
      .filter(Boolean);
  }

  return [];
}

function compactAreas(areas) {
  const safeAreas = toArray(areas);
  if (!safeAreas.length) return "—";
  return safeAreas.join(" / ");
}

function cleanActivityText(value) {
  const text = String(value ?? "").trim();
  return text.replace(/^二、\s*活動動態[:：]\s*/u, "");
}

function normalizeItem(item) {
  const normalized = { ...(item || {}) };

  normalized.intrusion_areas = toArray(normalized.intrusion_areas);

  if (!normalized.published_date && normalized.report_date) {
    normalized.published_date = normalized.report_date;
  }

  normalized.no_aircraft = Boolean(normalized.no_aircraft);

  return normalized;
}

function normalizeItems(items) {
  if (!Array.isArray(items)) return [];
  return items.map(normalizeItem);
}

function buildSummaryFromItems(items) {
  const safeItems = normalizeItems(items);
  const areaCounts = {};
  const noAircraftDates = [];

  safeItems.forEach((item) => {
    if (item?.no_aircraft && item?.published_date) {
      noAircraftDates.push(item.published_date);
    }

    toArray(item?.intrusion_areas).forEach((area) => {
      areaCounts[area] = (areaCounts[area] || 0) + 1;
    });
  });

  return {
    days_counted: safeItems.length,
    aircraft_total_sum: safeItems.reduce((sum, item) => sum + Number(item?.aircraft_total || 0), 0),
    aircraft_intrusion_total_sum: safeItems.reduce(
      (sum, item) => sum + Number(item?.aircraft_intrusion_total || 0),
      0
    ),
    ship_total_sum: safeItems.reduce((sum, item) => sum + Number(item?.ship_total || 0), 0),
    official_ship_total_sum: safeItems.reduce(
      (sum, item) => sum + Number(item?.official_ship_total || 0),
      0
    ),
    balloon_total_sum: safeItems.reduce((sum, item) => sum + Number(item?.balloon_total || 0), 0),
    no_aircraft_days: noAircraftDates.length,
    no_aircraft_dates: noAircraftDates,
    area_counts: areaCounts,
    recent_dates: safeItems.map((item) => item?.published_date).filter(Boolean),
  };
}

function normalizeSummary(payload, items) {
  const rawSummary = payload?.summary && typeof payload.summary === "object" ? payload.summary : null;

  const fallback = buildSummaryFromItems(items);

  if (!rawSummary || Number(rawSummary?.days_counted || 0) === 0) {
    return fallback;
  }

  const noAircraftDates = toArray(rawSummary?.no_aircraft_dates);

  return {
    ...fallback,
    ...rawSummary,
    no_aircraft_dates: noAircraftDates,
  };
}

function renderSummary(summary) {
  const noAircraftDates = toArray(summary?.no_aircraft_dates);
  const noAircraftTooltip = noAircraftDates.length
    ? `七日内无到访日期：${noAircraftDates.join("、")}`
    : "未到访日";
  const noAircraftDatesText = noAircraftDates.length ? noAircraftDates.join("、") : "无";

  return `
    <div class="mnd-pla-summary-heading">7日内家长到访统计</div>
    <div class="mnd-pla-summary-grid">
      <div class="mnd-pla-summary-card">
        <span>到访军机</span>
        <strong>${escapeHtml(formatInt(summary?.aircraft_total_sum))}</strong>
      </div>
      <div class="mnd-pla-summary-card">
        <span>深入到访</span>
        <strong>${escapeHtml(formatInt(summary?.aircraft_intrusion_total_sum))}</strong>
      </div>
      <div class="mnd-pla-summary-card">
        <span>到访军舰</span>
        <strong>${escapeHtml(formatInt(summary?.ship_total_sum))}</strong>
      </div>
      <div class="mnd-pla-summary-card">
        <span>公务船</span>
        <strong>${escapeHtml(formatInt(summary?.official_ship_total_sum))}</strong>
      </div>
      <div
        class="mnd-pla-summary-card mnd-pla-summary-card-accent"
        title="${escapeHtml(noAircraftTooltip)}"
      >
        <span>无到访日</span>
        <strong>${escapeHtml(formatInt(summary?.no_aircraft_days))}</strong>
        <div class="mnd-pla-no-aircraft-dates mnd-pla-mobile-only">${escapeHtml(noAircraftDatesText)}</div>
      </div>
    </div>
  `;
}

function renderDayCard(item) {
  const normalized = normalizeItem(item);
  const noAircraft = Boolean(normalized?.no_aircraft);

  return `
    <article class="mnd-pla-day-card ${noAircraft ? "is-no-aircraft" : ""}">
      <div class="mnd-pla-day-head">
        <div class="mnd-pla-day-date">${escapeHtml(normalized?.report_date || normalized?.published_date || "—")}</div>
        <div class="mnd-pla-day-period">${escapeHtml(normalized?.period_start || "—")} → ${escapeHtml(normalized?.period_end || "—")}</div>
      </div>
      <div class="mnd-pla-metrics">
        <div class="mnd-pla-metric"><span>到访军机</span><strong>${escapeHtml(formatInt(normalized?.aircraft_total))}</strong></div>
        <div class="mnd-pla-metric"><span>深入到访</span><strong>${escapeHtml(formatInt(normalized?.aircraft_intrusion_total))}</strong></div>
        <div class="mnd-pla-metric"><span>到访军舰</span><strong>${escapeHtml(formatInt(normalized?.ship_total))}</strong></div>
        <div class="mnd-pla-metric"><span>公务船</span><strong>${escapeHtml(formatInt(normalized?.official_ship_total))}</strong></div>
      </div>
      <div class="mnd-pla-subline">空域：${escapeHtml(compactAreas(normalized?.intrusion_areas))}</div>
      ${noAircraft ? '<div class="mnd-pla-flag">当日未侦获共机</div>' : ""}
      <div class="mnd-pla-copy">${escapeHtml(cleanActivityText(normalized?.activity_text || normalized?.body || ""))}</div>
    </article>
  `;
}

export function createMndPlaModule(ctx) {
  const container = document.getElementById("mndPlaContainer");
  const updatedAt = document.getElementById("mndPlaUpdatedAt");

  function updateHeader() {
    if (!updatedAt) return;

    if (!ctx.state.lastMndPlaFetchedAt) {
      updatedAt.textContent = "（暂无更新时间）";
      updatedAt.title = "";
      return;
    }

    updatedAt.textContent = `（${formatRelativeLocalTime(ctx.state.lastMndPlaFetchedAt)}更新）`;
    updatedAt.title = formatAbsoluteLocalDateTime(ctx.state.lastMndPlaFetchedAt);
  }

  function render(payload) {
    if (!container) return;

    const items = normalizeItems(payload?.items);
    const summary = normalizeSummary(payload, items);

    if (!items.length) {
      setEmpty(container, "最近 7 天暂无台海动态数据。");
      return;
    }

    const displayItems = items.slice(0, 2);

    clearElement(container);
    container.innerHTML = `
      <div class="mnd-pla-scroll">
        ${renderSummary(summary)}
        <div class="mnd-pla-day-list">
          ${displayItems.map((item) => renderDayCard(item)).join("")}
        </div>
      </div>
    `;
  }

  async function refresh() {
    setLoading(container, "正在加载台海动态...");
    try {
      const data = await ctx.api.get("/api/mnd-pla/dashboard?days=7");
      ctx.state.mndPlaData = data || { items: [], summary: {} };
      const exportGeneratedAt = await getExportProfileGeneratedAt("12h");
      ctx.state.lastMndPlaFetchedAt = exportGeneratedAt || data?.fetched_at || null;
      render(ctx.state.mndPlaData);
      updateHeader();
    } catch (error) {
      console.error("Failed to load MND PLA data:", error);
      ctx.state.lastMndPlaFetchedAt = null;
      updateHeader();
      setError(container, "台海动态加载失败。");
    }
  }

  return {
    render,
    refresh,
    updateHeader,
    init() {
    },
  };
}