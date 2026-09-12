import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";

const WMO_LIST_URL = "https://severeweather.wmo.int/list.html";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const WEATHER_COUNTRY_PREFIX_MAP = {
  us: "United States",
  hr: "Croatia",
  gr: "Greece",
  it: "Italy",
  in: "India",
  fr: "France",
  de: "Germany",
  es: "Spain",
  pt: "Portugal",
  uk: "United Kingdom",
  gb: "United Kingdom",
  ca: "Canada",
  au: "Australia",
  nz: "New Zealand",
  jp: "Japan",
  kr: "South Korea",
  cn: "China",
  tw: "Taiwan",
  hk: "Hong Kong",
  sg: "Singapore",
  my: "Malaysia",
  th: "Thailand",
  vn: "Vietnam",
  ph: "Philippines",
  id: "Indonesia",
  mx: "Mexico",
  br: "Brazil",
  ar: "Argentina",
  cl: "Chile",
  pe: "Peru",
  sb: "Solomon Islands",
};

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function inferWeatherCountry(item) {
  const capPath = firstNonEmpty(item?.payload?.cap_path, item?.payload?.capURL, item?.payload?.url);
  const mid = String(item?.payload?.mid || "").trim();
  const id = String(item?.payload?.id || "").trim();

  if (id.startsWith("urn:oid:2.49.0.1.840.")) return "United States";
  if (id.startsWith("IN-")) return "India";

  if (capPath) {
    const head = capPath.split("/")[0]?.toLowerCase() || "";
    const prefix = head.split("-")[0];
    if (WEATHER_COUNTRY_PREFIX_MAP[prefix]) return WEATHER_COUNTRY_PREFIX_MAP[prefix];
  }

  if (mid === "093") return "United States";
  if (mid === "066") return "India";
  if (mid === "063") return "Greece";
  if (mid === "019") return "Croatia";
  if (mid === "176") return "Italy";
  if (mid === "139") return "Solomon Islands";

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
        <div class="disaster-pane-empty-subtitle">这里只显示按系统规则筛选后的 extreme 天气警报。</div>
      </div>
    `;
  }

  const rows = items
    .map((item) => {
      const eventLabel = normalizeEventLabel(item);
      const region = normalizeRegion(item);
      const country = inferWeatherCountry(item);
      const subtitle = buildSubtitle(country, region);
      const publishedAt = firstNonEmpty(item?.published_at, item?.issued_at, item?.effective_at, item?.updated_at);
      const expiresAt = firstNonEmpty(item?.expires_at, item?.updated_at);
      const absoluteTitle = formatAbsoluteLocalDateTime(publishedAt);

      return `
        <a
          class="hazard-card hazard-card-weather hazard-card-link"
          href="${escapeHtml(WMO_LIST_URL)}"
          target="_blank"
          rel="noopener noreferrer"
          title="打开 WMO Severe Weather 列表"
        >
          <div class="hazard-card-main">
            <div class="hazard-card-topline">
              <span class="hazard-type-chip type-weather">即时警报</span>
              <span class="hazard-level-chip level-extreme">EXTREME</span>
              <span class="hazard-updated-text" title="${escapeHtml(absoluteTitle)}">${escapeHtml(formatRelativeLocalTime(publishedAt))}</span>
            </div>

            <div class="hazard-card-title">${escapeHtml(eventLabel)}</div>
            <div class="hazard-card-subtitle">${escapeHtml(subtitle)}</div>

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
