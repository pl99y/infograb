function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function cleanText(value) {
  return String(value ?? "").trim();
}

function fmtPercent(value) {
  if (value === null || value === undefined || value === "") return "—";
  return `${Number(value)}%`;
}

function fmtInt(value) {
  if (value === null || value === undefined || value === "") return "—";
  return String(Number(value));
}

function fmtDelay(value) {
  if (value === null || value === undefined || value === "") return "—";
  return `${Number(value)}m`;
}

function safeCountry(value) {
  const text = cleanText(value);
  if (!text || text.toLowerCase() === "unknown") return "";
  return text;
}

function buildTooltip(item) {
  const airport = cleanText(item?.airport_name);
  const country = safeCountry(item?.country);
  const region = cleanText(item?.region);
  const direction = cleanText(item?.direction);

  return [airport, country, region && direction ? `${region} · ${direction}` : region || direction]
    .filter(Boolean)
    .join(" · ");
}

function getSourceUrl(item) {
  return cleanText(item?.source_url)
    || cleanText(item?.url)
    || "https://www.flightradar24.com/data/airport-disruption?continent=worldwide&indices=true&period=live&type=departures";
}

function inferLocationLabel(item) {
  const airport = cleanText(item?.airport_name);
  if (!airport) return safeCountry(item?.country) || "—";

  let text = airport
    .replace(/\bInternational Airport\b/gi, "")
    .replace(/\bRegional Airport\b/gi, "")
    .replace(/\bMunicipal Airport\b/gi, "")
    .replace(/\bAirport\b/gi, "")
    .replace(/\bAeropuerto Internacional\b/gi, "")
    .replace(/\bAeropuerto\b/gi, "")
    .replace(/\bAeroporto Internacional\b/gi, "")
    .replace(/\bAeroporto\b/gi, "")
    .replace(/\bIntl\.?\b/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  if (!text) return safeCountry(item?.country) || airport || "—";
  return text;
}

function renderHead() {
  return `
    <div class="aviation-disruption-table-head">
      <div>IATA</div>
      <div class="aviation-disruption-table-head-metrics">
        <div>延误率</div>
        <div>取消率</div>
        <div>平均延误</div>
      </div>
    </div>
  `;
}

function renderOne(item) {
  const iata = cleanText(item?.iata).toUpperCase() || "—";
  const locationLabel = inferLocationLabel(item);
  const delayedFlights = fmtInt(item?.delayed_flights);
  const canceledFlights = fmtInt(item?.canceled_flights);
  const delayedPercent = fmtPercent(item?.delayed_percent);
  const canceledPercent = fmtPercent(item?.canceled_percent);
  const averageDelay = fmtDelay(item?.average_delay_min);
  const tooltip = buildTooltip(item);
  const href = getSourceUrl(item);

  return `
    <a class="aviation-card-link" href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">
      <article class="aviation-disruption-item aviation-disruption-item-clickable" ${tooltip ? `title="${escapeHtml(tooltip)}"` : ""}>
        <div class="aviation-disruption-code-wrap">
          <div class="aviation-disruption-code">${escapeHtml(iata)}</div>
          <div class="aviation-disruption-code-sub">${escapeHtml(locationLabel || "—")}</div>
        </div>

        <div class="aviation-disruption-metrics-compact">
          <div class="aviation-disruption-metric">
            <div class="aviation-disruption-metric-value">${escapeHtml(delayedPercent)}</div>
            <div class="aviation-disruption-metric-sub">${escapeHtml(delayedFlights)}</div>
          </div>

          <div class="aviation-disruption-metric">
            <div class="aviation-disruption-metric-value">${escapeHtml(canceledPercent)}</div>
            <div class="aviation-disruption-metric-sub">${escapeHtml(canceledFlights)}</div>
          </div>

          <div class="aviation-disruption-metric">
            <div class="aviation-disruption-metric-value">${escapeHtml(averageDelay)}</div>
            <div class="aviation-disruption-metric-sub">&nbsp;</div>
          </div>
        </div>
      </article>
    </a>
  `;
}

export function renderAviationDisruptions(container, items) {
  if (!container) return;

  if (!Array.isArray(items) || items.length === 0) {
    container.innerHTML = `<div class="empty-state">暂无机场扰动数据</div>`;
    return;
  }

  container.innerHTML = renderHead() + items.map(renderOne).join("");
}

export const renderDisruptions = renderAviationDisruptions;
export default renderAviationDisruptions;
