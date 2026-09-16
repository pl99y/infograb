import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";

const USGS_MAP_URL = "https://earthquake.usgs.gov/earthquakes/map/";
const MIROVA_URL = "https://www.mirovaweb.it/NRT/";
const TSUNAMI_URL = "https://www.tsunami.gov/";
const JTWC_ABIO_URL = "https://www.metoc.navy.mil/jtwc/products/abioweb.txt";
const JTWC_ABPW_URL = "https://www.metoc.navy.mil/jtwc/products/abpwweb.txt";
const NHC_AT_URL = "https://www.nhc.noaa.gov/text/MIATWOAT.shtml?text=";
const NHC_EP_URL = "https://www.nhc.noaa.gov/text/MIATWOEP.shtml?text=";
const CPHC_CP_URL = "https://www.nhc.noaa.gov/text/HFOTWOCP.shtml?text=";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function formatMetric(value, suffix = "") {
  if (value === null || value === undefined || value === "") return "";
  const n = Number(value);
  if (Number.isFinite(n)) {
    return `${Number.isInteger(n) ? n.toFixed(0) : n.toFixed(2)}${suffix}`;
  }
  return `${value}${suffix}`;
}

function titleCase(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.charAt(0).toUpperCase() + text.slice(1).toLowerCase();
}

function typeLabel(eventType) {
  const m = { earthquake: "地震", tsunami: "海啸", volcano: "火山", typhoon: "台风", flood: "洪水" };
  return m[eventType] || eventType || "灾害";
}

function severityClass(level) {
  const v = String(level || "").toLowerCase();
  if (["critical", "red", "high", "very_high", "very-high", "extreme"].includes(v)) return "level-high";
  if (["moderate", "orange", "medium", "elevated"].includes(v)) return "level-mid";
  if (["low", "green", "info"].includes(v)) return "level-low";
  return "level-neutral";
}

function displayTime(item) {
  return firstNonEmpty(
    item?.display_time,
    item?.primary_time,
    item?.occurred_at,
    item?.started_at,
    item?.published_at,
    item?.updated_at,
    item?.fetched_at,
  );
}

function typhoonSourceUrl(item) {
  const sourcePrimary = String(item?.source_primary || "").trim().toUpperCase();
  const source = String(item?.payload?.source || "").trim().toLowerCase();

  if (sourcePrimary === "JTWC-ABIO" || source === "jtwc_abio") return JTWC_ABIO_URL;
  if (sourcePrimary === "JTWC-ABPW" || source === "jtwc_abpw") return JTWC_ABPW_URL;
  if (sourcePrimary === "NHC-AT" || source === "nhc_at") return NHC_AT_URL;
  if (sourcePrimary === "NHC-EP" || source === "nhc_ep") return NHC_EP_URL;
  if (sourcePrimary === "CPHC-CP" || source === "cphc_cp") return CPHC_CP_URL;

  const secondary = String(item?.source_secondary || "").trim().toLowerCase();
  if (secondary.includes("abio")) return JTWC_ABIO_URL;
  if (secondary.includes("abpw")) return JTWC_ABPW_URL;
  return "";
}

function officialUrl(item) {
  const eventType = String(item?.event_type || "").toLowerCase();
  const payload = item?.payload || {};

  if (eventType === "typhoon") {
    return (
      typhoonSourceUrl(item) ||
      firstNonEmpty(
        payload?.source_url,
        payload?.official_link,
        payload?.bulletin_url,
        item?.official_link,
      )
    );
  }

  if (eventType === "earthquake") return USGS_MAP_URL;
  if (eventType === "volcano") return MIROVA_URL;
  if (eventType === "tsunami") {
    return firstNonEmpty(
      payload?.bulletin_url,
      item?.official_link,
      payload?.official_link,
      TSUNAMI_URL,
    );
  }
  if (eventType === "flood") {
    return firstNonEmpty(
      item?.official_link,
      payload?.official_link,
      payload?.report_url,
      payload?.details_url,
      item?.map_url,
    );
  }

  return firstNonEmpty(
    item?.official_link,
    payload?.official_link,
    payload?.bulletin_url,
    payload?.report_url,
    payload?.details_url,
    payload?.source_url,
  );
}

function mapUrl(item) {
  const eventType = String(item?.event_type || "").toLowerCase();
  if (eventType === "earthquake") return USGS_MAP_URL;
  if (eventType === "volcano") return MIROVA_URL;
  return firstNonEmpty(item?.map_url, item?.payload?.map_url, officialUrl(item));
}

function buildSubtitle(item) {
  const eventType = String(item?.event_type || "").toLowerCase();
  if (eventType === "typhoon" || eventType === "volcano") return "";

  const title = firstNonEmpty(item?.title);
  const subtitle = firstNonEmpty(item?.summary, item?.location_text);
  if (!subtitle) return "";
  if (subtitle.trim() === title.trim()) return "";
  return subtitle;
}

function buildInlineFacts(item) {
  const type = String(item?.event_type || "").toLowerCase();
  const payload = item?.payload || {};
  const facts = [];

  function pushFact(label, value) {
    const text = firstNonEmpty(value);
    if (!text) return;
    facts.push({ label, value: text });
  }

  if (type === "earthquake") {
    pushFact("震级", formatMetric(payload.mag, " M"));
    pushFact("深度", formatMetric(payload.depth_km, " km"));
    pushFact("位置", firstNonEmpty(item.location_text, payload.place));
  } else if (type === "volcano") {
    pushFact("VRP", formatMetric(payload.vrp_mw, " MW"));
    pushFact("传感器", firstNonEmpty(item.source_secondary, payload.sensor));
    pushFact("距离", formatMetric(payload.distance_km, " km"));
  } else if (type === "flood") {
    pushFact("地区", firstNonEmpty(item.location_text, payload?.raw?.country, item.summary));
    pushFact("等级", titleCase(item.severity_level || payload.alertlevel));
    pushFact("状态", titleCase(item.status));
  } else if (type === "typhoon") {
    pushFact("类型", firstNonEmpty(payload.item_type, item.source_secondary));
    pushFact("位置", firstNonEmpty(item.location_text, payload.platform_location));
    pushFact("风速", firstNonEmpty(payload.wind, payload.platform_summary?.match(/winds?\s+([^|]+)/i)?.[1]?.trim()));
    pushFact("气压", payload.metadata?.pressure_mb ? `${payload.metadata.pressure_mb} mb` : "");
    pushFact("风险", firstNonEmpty(item.status, payload.metadata?.development_level));
  } else if (type === "tsunami") {
    pushFact("区域", firstNonEmpty(item.location_text, item.summary));
    pushFact("状态", titleCase(item.status || item.severity_level));
  } else {
    pushFact("信息", firstNonEmpty(item.summary, item.location_text));
  }

  return facts.slice(0, 5);
}

function renderInlineFacts(item) {
  const facts = buildInlineFacts(item);
  if (!facts.length) return "";
  return `
    <div class="hazard-inline-meta">
      ${facts
        .map(
          (fact) => `
            <span class="hazard-inline-item">
              <span class="hazard-inline-key">${escapeHtml(fact.label)}</span>
              <span class="hazard-inline-value">${escapeHtml(fact.value)}</span>
            </span>
          `,
        )
        .join("")}
    </div>
  `;
}

export function renderDisasterPane(items = []) {
  if (!Array.isArray(items) || items.length === 0) {
    return `
      <div class="disaster-pane-empty">
        <div class="disaster-pane-empty-title">当前暂无自然灾害项</div>
        <div class="disaster-pane-empty-subtitle">地震、海啸、火山、台风、洪水会显示在这里。</div>
      </div>
    `;
  }

  const rows = items.map((item) => {
    const eventType = String(item?.event_type || "unknown").toLowerCase();
    const level = firstNonEmpty(item?.severity_level, item?.payload?.alertlevel, item?.payload?.alert_level);
    const title = firstNonEmpty(item?.title, `${typeLabel(eventType)}事件`);
    const subtitle = buildSubtitle(item);
    const source = firstNonEmpty(item?.source_primary, item?.source, item?.payload?.source);
    const latestTime = displayTime(item);
    const sevClass = severityClass(level);
    const cardUrl = officialUrl(item);
    const cardUrlAttr = cardUrl ? ` data-card-url="${escapeHtml(cardUrl)}" tabindex="0" role="link"` : "";
    const mapHref = mapUrl(item);

    return `
      <article class="hazard-card hazard-card-disaster hazard-card-compact ${sevClass}${cardUrl ? " hazard-card-clickable" : ""}"${cardUrlAttr}>
        ${mapHref ? `<a class="hazard-map-btn" href="${escapeHtml(mapHref)}" target="_blank" rel="noopener noreferrer" data-hazard-map-link>地图</a>` : ""}

        <div class="hazard-card-main">
          <div class="hazard-card-topline hazard-card-topline-compact">
            <span class="hazard-type-chip type-${escapeHtml(eventType)}">${escapeHtml(typeLabel(eventType))}</span>
            ${level ? `<span class="hazard-level-chip ${sevClass}">${escapeHtml(String(level).toUpperCase())}</span>` : ""}
            ${source ? `<span class="hazard-source-chip">${escapeHtml(source)}</span>` : ""}
            ${latestTime ? `<span class="hazard-updated-text" title="${escapeHtml(formatAbsoluteLocalDateTime(latestTime))}">${escapeHtml(formatRelativeLocalTime(latestTime))}</span>` : ""}
          </div>

          <div class="hazard-card-title">${escapeHtml(title)}</div>
          ${subtitle ? `<div class="hazard-card-subtitle hazard-card-subtitle-compact">${escapeHtml(subtitle)}</div>` : ""}
          ${renderInlineFacts(item)}
        </div>
      </article>
    `;
  }).join("");

  return `<div class="hazard-card-list disaster-card-list">${rows}</div>`;
}
