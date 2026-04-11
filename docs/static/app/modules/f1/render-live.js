const STATUS_LABELS = {
  live: "LIVE",
  scheduled: "SCHEDULED",
  stopped: "STOPPED",
  finished: "FINISHED",
};

const GP_NAME_ALIASES = {
  Australia: "Australian Grand Prix",
  China: "Chinese Grand Prix",
  Japan: "Japan Grand Prix",
  Bahrain: "Bahrain Grand Prix",
  "Saudi Arabia": "Saudi Arabian Grand Prix",
  Miami: "Miami Grand Prix",
  "Emilia-Romagna": "Emilia-Romagna Grand Prix",
  Monaco: "Monaco Grand Prix",
  Spain: "Spanish Grand Prix",
  Canada: "Canadian Grand Prix",
  Austria: "Austrian Grand Prix",
  "Great Britain": "British Grand Prix",
  Belgium: "Belgian Grand Prix",
  Hungary: "Hungarian Grand Prix",
  Netherlands: "Dutch Grand Prix",
  Italy: "Italian Grand Prix",
  Azerbaijan: "Azerbaijan Grand Prix",
  Singapore: "Singapore Grand Prix",
  "United States": "United States Grand Prix",
  Mexico: "Mexico City Grand Prix",
  "São Paulo": "São Paulo Grand Prix",
  "Sao Paulo": "São Paulo Grand Prix",
  "Las Vegas": "Las Vegas Grand Prix",
  Qatar: "Qatar Grand Prix",
  "Abu Dhabi": "Abu Dhabi Grand Prix",
};

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function cleanGrandPrixName(value) {
  const original = String(value ?? "").trim();
  if (!original) return "F1";

  const cleaned = original
    .replace(/^FORMULA\s*1\s*[:\-–—]?\s*/i, "")
    .replace(/\s*[-–—]\s*(Race|Sprint|Qualifying|Sprint Qualifying|Practice\s*\d+)\s*$/i, "")
    .replace(/\s*\([^)]*\)\s*$/i, (match) => (/grand prix/i.test(original) ? "" : match))
    .trim();

  if (/grand prix/i.test(cleaned)) return cleaned;
  return GP_NAME_ALIASES[cleaned] || `${cleaned} Grand Prix`;
}

export function inferStatusClass(payload) {
  if (!payload) return "finished";
  const mode = (payload.mode || "").toLowerCase();
  const sessionName = ((payload.session_name || payload.session?.session_name || "") + "").toLowerCase();

  if (mode === "live") return "live";
  if (sessionName.includes("cancel")) return "stopped";
  if (mode === "between_rounds") return "finished";
  return "scheduled";
}

export function getF1StatusLabel(payload) {
  const statusClass = inferStatusClass(payload);
  return STATUS_LABELS[statusClass] || STATUS_LABELS.finished;
}

export function getF1GrandPrixName(payload) {
  const roundName = payload?.round?.name || payload?.session?.round_name || payload?.headline || payload?.session?.title || "F1";
  return cleanGrandPrixName(roundName);
}

export function getF1LiveStatusMarkup(payload, options = {}) {
  if (!payload || payload.error) {
    return `<span class="f1-refresh-error">F1 数据不可用</span>`;
  }

  const gpName = getF1GrandPrixName(payload);
  const statusClass = inferStatusClass(payload);
  const statusLabel = getF1StatusLabel(payload);
  const updatedText = String(options?.updatedText || "").trim();
  const updatedTitle = String(options?.updatedTitle || "").trim();

  return `
    <span class="f1-live-status-inline">
      <strong class="f1-live-status-name">${esc(gpName)}</strong>
      <span class="f1-live-status-pill f1-live-status-pill-${esc(statusClass)}">${esc(statusLabel)}</span>
      ${updatedText ? `<span class="f1-live-updated"${updatedTitle ? ` title="${esc(updatedTitle)}"` : ""}>${esc(updatedText)}</span>` : ""}
    </span>
  `;
}

function buildResultCell(row) {
  const time = row?.result_time || "";
  const gap = row?.gap || "";
  const laps = row?.laps ?? "";
  const parts = [];
  if (time) parts.push(`<span class="f1-live-result-strong">${esc(time)}</span>`);
  if (gap && gap !== "+0.000") parts.push(`<span>${esc(gap)}</span>`);
  if (String(laps) !== "") parts.push(`<span>${esc(laps)} laps</span>`);
  return parts.join(" • ") || '<span class="f1-live-result-muted">—</span>';
}

function buildLiveRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return `<div class="f1-empty">No F1 result rows are available right now.</div>`;
  }

  const head = `
    <div class="f1-live-table-head">
      <div>POS</div>
      <div>DRIVER</div>
      <div>TEAM</div>
      <div>RESULT</div>
    </div>
  `;

  const items = rows
    .map((row) => {
      const pos = row?.position ?? "—";
      const driver = row?.driver || "Unknown";
      const driverCode = row?.driver_code ? ` <span class="f1-live-mini">(${esc(row.driver_code)})</span>` : "";
      const team = row?.team || "—";
      const nation = row?.nation ? `<div class="f1-live-mini">${esc(row.nation)}</div>` : "";
      return `
        <div class="f1-live-row">
          <div class="f1-live-pos">${esc(pos)}</div>
          <div class="f1-live-driver-wrap">
            <div class="f1-live-driver">${esc(driver)}${driverCode}</div>
            ${nation}
          </div>
          <div class="f1-live-team">${esc(team)}</div>
          <div class="f1-live-result">${buildResultCell(row)}</div>
        </div>
      `;
    })
    .join("");

  return `${head}<div class="f1-live-list">${items}</div>`;
}

export function renderF1Live(targetEl, payload) {
  if (!targetEl) return;

  if (!payload || payload.error) {
    targetEl.innerHTML = `
      <div class="f1-empty">
        ${esc(payload?.error || "F1 live data is unavailable right now.")}
      </div>
    `;
    return;
  }

  targetEl.innerHTML = buildLiveRows(payload.rows || []);
}
