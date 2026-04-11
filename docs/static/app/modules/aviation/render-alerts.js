function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function cleanText(value) {
  return String(value ?? "").trim();
}

function normalizeLabel(value) {
  return cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fff]+/g, " ")
    .trim();
}

function isPlaceholder(value) {
  const normalized = normalizeLabel(value);
  return ["", "n a", "na", "unknown", "null", "none", "status n a", "std", "origin", "destination", "fecha", "vuelo"].includes(normalized);
}

function isSuspiciousAlert(item) {
  const callsign = normalizeLabel(item?.callsign);
  const dep = normalizeLabel(item?.departure_airport);
  const arr = normalizeLabel(item?.arrival_airport);
  const status = normalizeLabel(item?.status_text);

  const blocked = new Set(["vuelo", "fecha", "origin", "destination", "std", "sta", "flight", "alerts"]);
  if (blocked.has(callsign)) return true;
  if (blocked.has(dep) || blocked.has(arr)) return true;
  if (callsign.length <= 1 && !cleanText(item?.aircraft_text) && !cleanText(item?.duration_text)) return true;
  if (callsign === "unknown" && isPlaceholder(dep) && isPlaceholder(arr) && isPlaceholder(status)) return true;
  return false;
}

function getSquawkCode(item) {
  const direct = cleanText(item?.squawk_code);
  if (direct) return direct.toUpperCase();
  const alertType = cleanText(item?.alert_type);
  return alertType ? alertType.split("-")[0].trim().toUpperCase() : "";
}

function getBadgeClass(code) {
  switch (code) {
    case "7700": return "aviation-badge aviation-badge-emergency";
    case "7600": return "aviation-badge aviation-badge-lostcomm";
    case "7500": return "aviation-badge aviation-badge-hijack";
    default: return "aviation-badge aviation-badge-emergency";
  }
}

function getRoutePart(item, kind) {
  const direct = kind === "departure" ? cleanText(item?.departure_airport) : cleanText(item?.arrival_airport);
  if (direct && !isPlaceholder(direct)) return direct;

  const extra = item?.extra || {};
  const rawCells = Array.isArray(extra.raw_cells) ? extra.raw_cells : [];
  const fallbackIndex = kind === "departure" ? 2 : 4;
  const fallback = cleanText(rawCells[fallbackIndex]);
  if (fallback && !isPlaceholder(fallback)) return fallback;

  return "";
}

function formatRoute(item) {
  const dep = getRoutePart(item, "departure");
  const arr = getRoutePart(item, "arrival");
  if (dep && arr) return `${dep} → ${arr}`;
  if (dep) return dep;
  if (arr) return arr;
  return "路线信息缺失";
}

function formatStatus(item) {
  const status = cleanText(item?.status_text);
  const alertType = cleanText(item?.alert_type);
  if (status && alertType && !isPlaceholder(status)) return `${status} · ${alertType}`;
  return (!isPlaceholder(status) ? status : "") || alertType || "状态未知";
}

function buildMetaHtml(item) {
  const fields = [
    ["机型", cleanText(item?.aircraft_text)],
    ["起飞", cleanText(item?.departure_time_text)],
    ["到达", cleanText(item?.arrival_time_text)],
    ["时长", cleanText(item?.duration_text)],
    ["距离", cleanText(item?.distance_text)],
    ["来源", cleanText(item?.source_name) || "AirNav Radar"],
  ];

  return fields
    .filter(([, value]) => value && !isPlaceholder(value))
    .map(([label, value]) => `
      <div class="aviation-meta-item">
        <span class="aviation-meta-label">${escapeHtml(label)}:</span>${escapeHtml(value)}
      </div>
    `)
    .join("");
}

function getSourceUrl(item) {
  return cleanText(item?.source_url) || cleanText(item?.url) || "https://www.airnavradar.com/data/alerts";
}

function renderOneAlert(item) {
  const callsign = cleanText(item?.callsign) || cleanText(item?.registration) || "UNKNOWN";
  const route = formatRoute(item);
  const status = formatStatus(item);
  const eventDate = cleanText(item?.event_date_text);
  const badgeText = getSquawkCode(item);
  const badgeClass = getBadgeClass(badgeText);
  const metaHtml = buildMetaHtml(item);
  const href = getSourceUrl(item);

  return `
    <a class="aviation-card-link" href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">
      <article class="aviation-alert-item aviation-alert-item-clickable">
        <div class="aviation-alert-head">
          <div class="aviation-alert-head-left">
            <div class="aviation-alert-id-label">注册号 / 呼号</div>
            <div class="aviation-alert-callsign">${escapeHtml(callsign)}</div>
          </div>

          <div class="aviation-alert-head-right">
            ${eventDate ? `<div class="aviation-alert-date">${escapeHtml(eventDate)}</div>` : ""}
            ${badgeText ? `<span class="${badgeClass}">${escapeHtml(badgeText)}</span>` : ""}
          </div>
        </div>

        <div class="aviation-alert-route">${escapeHtml(route)}</div>
        <div class="aviation-alert-status">${escapeHtml(status)}</div>
        ${metaHtml ? `<div class="aviation-alert-meta">${metaHtml}</div>` : ""}
      </article>
    </a>
  `;
}

export function renderAviationAlerts(container, items) {
  if (!container) return;
  if (!Array.isArray(items) || items.length === 0) {
    container.innerHTML = `<div class="empty-state">暂无异常航班</div>`;
    return;
  }

  const filtered = items.filter((item) => !isSuspiciousAlert(item));
  if (filtered.length === 0) {
    container.innerHTML = `<div class="empty-state">暂无可显示的异常航班</div>`;
    return;
  }

  container.innerHTML = filtered.map(renderOneAlert).join("");
}

export const renderAlerts = renderAviationAlerts;
export default renderAviationAlerts;
