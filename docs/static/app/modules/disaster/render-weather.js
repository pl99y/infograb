import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js?v=46";

const WMO_LIST_URL = "https://severeweather.wmo.int/list.html";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const regionNames = new Intl.DisplayNames(["zh-CN"], { type: "region", fallback: "none" });

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function inferWeatherCountry(item) {
  const path = firstNonEmpty(item?.payload?.cap_path, item?.payload?.capURL, item?.payload?.url);
  const code = path.match(/^(?:https:\/\/[^/]+\/)?([a-z]{2})-[^/]+\//i)?.[1]?.toUpperCase();
  if (code === "UK") return regionNames.of("GB");
  if (code && code !== "ZZ") return regionNames.of(code) || "";
  const id = String(item?.payload?.id || "");
  if (id.startsWith("urn:oid:2.49.0.1.840.")) return regionNames.of("US");
  if (id.startsWith("IN-")) return regionNames.of("IN");
  return "";

}

function normalizeEventLabel(item) {
  return firstNonEmpty(item?.payload?.event, item?.event_type === "weather" ? "Weather Alert" : "Alert");
}

function normalizeRegion(item) {
  return firstNonEmpty(
    item?.location_text,
    item?.summary,
    item?.region_text,
    item?.payload?.areaDesc,
    item?.payload?.area_desc,
  );
}

function buildSubtitle(country, region) {
  if (country && region && country.toLowerCase() !== region.toLowerCase()) {
    return `${country} · ${region}`;
  }
  return firstNonEmpty(region, country, "区域待确认");
}

function renderTimeRow(label, value) {
  if (!value) return "";
  const absolute = formatAbsoluteLocalDateTime(value);
  return `
    <div class="hazard-meta-row hazard-meta-time" title="${escapeHtml(absolute)}">
      <span class="hazard-meta-key">${escapeHtml(label)}</span>
      <span class="hazard-meta-value">${escapeHtml(formatRelativeLocalTime(value))}</span>
    </div>
  `;
}

export function renderWeatherPane(items = []) {
  if (!Array.isArray(items) || items.length === 0) {
    return `
      <div class="disaster-pane-empty">
        <div class="disaster-pane-empty-title">当前暂无即时警报</div>
      </div>
    `;
  }

  const rows = items
    .map((item) => {
      const eventLabel = firstNonEmpty(item?.display_zh?.title, normalizeEventLabel(item));
      const region = firstNonEmpty(item?.display_zh?.region, normalizeRegion(item));
      const country = inferWeatherCountry(item);
      const subtitle = buildSubtitle(country, region);
      const publishedAt = firstNonEmpty(item?.published_at, item?.issued_at, item?.effective_at, item?.updated_at);
      const expiresAt = firstNonEmpty(item?.expires_at);
      const absoluteTitle = formatAbsoluteLocalDateTime(publishedAt);

      return `
        <a
          class="hazard-card hazard-card-weather hazard-card-link"
          href="${escapeHtml(WMO_LIST_URL)}"
          target="_blank"
          rel="noopener noreferrer"
          title="${escapeHtml(buildSubtitle(normalizeEventLabel(item), normalizeRegion(item)))}"
        >
          <div class="hazard-card-main">
            <div class="hazard-card-topline">
              <span class="hazard-type-chip type-weather">即时警报</span>
              <span class="hazard-level-chip level-extreme">最高等级</span>
              <span class="hazard-updated-text" title="${escapeHtml(absoluteTitle)}">${escapeHtml(formatRelativeLocalTime(publishedAt))}</span>
            </div>

            <div class="hazard-card-title">${escapeHtml(eventLabel)}</div>
            <div class="hazard-card-subtitle">${escapeHtml(subtitle)}</div>
            ${item?.display_zh?.summary ? `<div class="hazard-card-subtitle">${escapeHtml(item.display_zh.summary)}</div>` : ""}

            <div class="hazard-meta-grid weather-meta-grid">
              ${renderTimeRow("发布", publishedAt)}
              ${renderTimeRow("失效", expiresAt)}
            </div>
          </div>
        </a>
      `;
    })
    .join("");

  return `<div class="hazard-card-list weather-card-list">${rows}</div>`;
}
