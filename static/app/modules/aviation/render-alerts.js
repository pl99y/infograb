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

function isAdsbfiAlert(item) {
  const source = normalizeLabel(item?.source_name);
  const parser = normalizeLabel(item?.extra?.parser_version);
  return source.includes("adsb fi") || source.includes("adsbfi") || parser.includes("adsbfi");
}

function isSuspiciousAlert(item) {
  if (isAdsbfiAlert(item)) return false;

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

function formatPosition(item) {
  const extra = item?.extra || {};
  const lat = extra.lat == null || extra.lat === "" ? NaN : Number(extra.lat);
  const lon = extra.lon == null || extra.lon === "" ? NaN : Number(extra.lon);
  if (Number.isFinite(lat) && Number.isFinite(lon)) {
    return `位置 ${lat.toFixed(3)}, ${lon.toFixed(3)}`;
  }
  return "";
}

function formatRoute(item) {
  if (isAdsbfiAlert(item)) {
    return formatPosition(item) || "实时位置未提供";
  }

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
  if (isAdsbfiAlert(item)) {
    if (item?.extra?.verification_pending) return "数据源暂不可用 · 当前状态未确认";
    const active = item?.extra?.is_active !== false;
    const prefix = active ? "最近采集仍检测到" : "本轮未再检测到";
    const meaning = { "7700": "一般紧急情况", "7600": "通信故障", "7500": "非法干扰" }[getSquawkCode(item)];
    return `${prefix}${meaning ? ` · ${meaning}` : ""}`;
  }

  if (status && alertType && !isPlaceholder(status)) return `${status} · ${alertType}`;
  return (!isPlaceholder(status) ? status : "") || alertType || "状态未知";
}

function formatIsoTime(value) {
  if (!value) return "";
  const text = cleanText(value);
  const date = typeof value === "number" ? new Date(value) : new Date(text);
  if (Number.isNaN(date.getTime())) return text;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatAltitude(value) {
  const text = cleanText(value);
  if (!text) return "";
  if (text.toLowerCase() === "ground") return "Ground";
  const n = Number(text);
  if (Number.isFinite(n)) return `${Math.round(n).toLocaleString()} ft`;
  return text;
}

function formatNumberWithUnit(value, unit, digits = 0) {
  if (value == null || cleanText(value) === "") return "";
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return `${n.toFixed(digits)} ${unit}`;
}

function buildAdsbfiMetaHtml(item) {
  const extra = item?.extra || {};
  const fields = [
    ["注册号", cleanText(extra.registration)],
    ["机型", cleanText(item?.aircraft_text) || cleanText(extra.description)],
    ["高度", formatAltitude(extra.alt_baro ?? extra.alt_geom)],
    ["速度", formatNumberWithUnit(extra.ground_speed_kt, "kt", 0)],
    ["航向", formatNumberWithUnit(extra.track_deg, "°", 0)],
    ["垂直速度", formatNumberWithUnit(extra.vertical_rate_fpm, "fpm", 0)],
    ["首次检测", formatIsoTime(extra.first_seen_utc)],
    ["最近检测", formatIsoTime(extra.last_seen_utc || item?.fetched_at)],
    ["HEX", cleanText(extra.hex).toUpperCase()],
    ["来源", cleanText(item?.source_name) || "ADSB.fi"],
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

function buildAirnavMetaHtml(item) {
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

function buildMetaHtml(item) {
  return isAdsbfiAlert(item) ? buildAdsbfiMetaHtml(item) : buildAirnavMetaHtml(item);
}

function getSourceUrl(item) {
  if (isAdsbfiAlert(item)) {
    const direct = cleanText(item?.source_url) || cleanText(item?.url);
    if (direct) return direct;
    const hex = cleanText(item?.extra?.hex);
    if (hex) return `https://globe.adsb.fi/?icao=${encodeURIComponent(hex)}`;
    return "https://globe.adsb.fi/";
  }

  return cleanText(item?.source_url) || cleanText(item?.url) || "https://www.airnavradar.com/data/alerts";
}

export function alertIdentity(item) {
  const extra = item?.extra || {};
  if (!isAdsbfiAlert(item)) return { value: cleanText(item?.callsign) || "未知", label: "航班标识" };
  const registration = cleanText(extra.registration).toUpperCase();
  const hex = cleanText(extra.hex).toUpperCase();
  const raw = extra.raw_aircraft;
  // Older snapshots already retain the source's flight field in raw_aircraft.
  const broadcast = cleanText(extra.broadcast_callsign ?? raw?.flight).replace(/\s+/g, "").toUpperCase();
  if (broadcast && !isPlaceholder(broadcast)) return { value: broadcast, label: "呼号" };
  if (!raw && extra.broadcast_callsign == null) {
    const legacy = cleanText(item?.callsign);
    if (legacy && legacy !== registration && legacy !== hex && !isPlaceholder(legacy)) return { value: legacy, label: "呼号" };
  }
  if (registration) return { value: registration, label: "注册号（未提供呼号）" };
  if (hex) return { value: hex, label: "HEX（未提供呼号）" };
  return { value: "未知", label: "未提供航空器标识" };
}

export function alertTimestamp(item) {
  for (const value of [item?.extra?.last_seen_utc, item?.fetched_at]) {
    if (value && Number.isFinite(new Date(value).getTime())) return new Date(value).getTime();
  }
  return 0;
}

function renderOneAlert(item) {
  const extra = item?.extra || {};
  const identity = alertIdentity(item);
  const callsign = identity.value;
  const route = formatRoute(item);
  const status = formatStatus(item);
  const eventDate = formatIsoTime(alertTimestamp(item)) || cleanText(item?.event_date_text);
  const badgeText = getSquawkCode(item);
  const badgeClass = getBadgeClass(badgeText);
  const metaHtml = buildMetaHtml(item);
  const href = getSourceUrl(item);
  const activeClass = isAdsbfiAlert(item) && item?.extra?.is_active === false ? " aviation-alert-item-inactive" : "";

  return `
    <a class="aviation-card-link" href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">
      <article class="aviation-alert-item aviation-alert-item-clickable${activeClass}">
        <div class="aviation-alert-head">
          <div class="aviation-alert-head-left">
            <div class="aviation-alert-id-label">${escapeHtml(identity.label)}</div>
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
    container.innerHTML = `
      <div class="empty-state">
        最近 48 小时未检测到 7500 / 7600 / 7700 异常应答机事件。<br>
        ADSB.fi API 正常时，空列表代表当前没有记录到相关异常航班。
      </div>
    `;
    return;
  }

  const filtered = items.filter((item) => !isSuspiciousAlert(item));
  if (filtered.length === 0) {
    container.innerHTML = `<div class="empty-state">最近 48 小时未检测到可显示的异常应答机事件。</div>`;
    return;
  }

  filtered.sort((a, b) => alertTimestamp(b) - alertTimestamp(a) || Number(b.id || 0) - Number(a.id || 0));
  container.innerHTML = filtered.map(renderOneAlert).join("");
}

export const renderAlerts = renderAviationAlerts;
export default renderAviationAlerts;
