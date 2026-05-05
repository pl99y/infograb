const STATUS_LABELS = {
  live: "LIVE",
  scheduled: "SCHEDULED",
  stopped: "STOPPED",
  finished: "FINISHED",
  between_rounds: "IDLE",
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

function isBetweenRounds(payload) {
  return String(payload?.mode || "").toLowerCase() === "between_rounds";
}

function getStrategyRound(payload, key) {
  const value = payload?.strategy?.[key];
  return value && typeof value === "object" ? value : null;
}

function getNextRound(payload) {
  return (
    getStrategyRound(payload, "next_round") ||
    getStrategyRound(payload, "primary_target") ||
    getStrategyRound(payload, "best_available_flashscore_target") ||
    (payload?.round && typeof payload.round === "object" ? payload.round : null)
  );
}

function getPreviousRound(payload) {
  return getStrategyRound(payload, "previous_round") || getStrategyRound(payload, "fallback_results");
}

function formatRoundDate(round) {
  if (!round || typeof round !== "object") return "";
  const dateText = String(round.date_text || "").trim();
  if (dateText) return dateText;
  const start = String(round.start_date || "").trim();
  const end = String(round.end_date || "").trim();
  if (start && end && start !== end) return `${start} → ${end}`;
  return start || end;
}

export function inferStatusClass(payload) {
  if (!payload) return "finished";
  const mode = (payload.mode || "").toLowerCase();
  const sessionName = ((payload.session_name || payload.session?.session_name || "") + "").toLowerCase();

  if (mode === "live") return "live";
  if (mode === "between_rounds") return "between_rounds";
  if (sessionName.includes("cancel")) return "stopped";
  return "scheduled";
}

export function getF1StatusLabel(payload) {
  const statusClass = inferStatusClass(payload);
  return STATUS_LABELS[statusClass] || STATUS_LABELS.finished;
}

export function getF1GrandPrixName(payload) {
  if (isBetweenRounds(payload)) {
    const nextRound = getNextRound(payload);
    return cleanGrandPrixName(nextRound?.name || "F1");
  }
  const roundName = payload?.round?.name || payload?.session?.round_name || payload?.headline || payload?.session?.title || "F1";
  return cleanGrandPrixName(roundName);
}

export function getF1LiveStatusMarkup(payload, options = {}) {
  if (isBetweenRounds(payload)) {
    const nextRound = getNextRound(payload);
    const nextName = nextRound?.name ? cleanGrandPrixName(nextRound.name) : "F1";
    const updatedText = String(options?.updatedText || "").trim();
    const updatedTitle = String(options?.updatedTitle || "").trim();
    return `
      <span class="f1-live-status-inline">
        <strong class="f1-live-status-name">${esc(nextName)}</strong>
        <span class="f1-live-status-pill f1-live-status-pill-scheduled">IDLE</span>
        ${updatedText ? `<span class="f1-live-updated"${updatedTitle ? ` title="${esc(updatedTitle)}"` : ""}>${esc(updatedText)}</span>` : ""}
      </span>
    `;
  }

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


function buildPreviousResultCell(row) {
  const time = row?.time_or_gap || row?.result_time || "";
  const status = row?.status || "";
  const points = row?.points ?? "";
  const parts = [];
  if (time) parts.push(`<span class="f1-live-result-strong">${esc(time)}</span>`);
  if (status && status !== "Finished") parts.push(`<span>${esc(status)}</span>`);
  if (String(points) !== "") parts.push(`<span>${esc(points)} pts</span>`);
  return parts.join(" • ") || '<span class="f1-live-result-muted">—</span>';
}

function buildPreviousResultMarkup(previousResult) {
  if (!previousResult || previousResult.ok !== true || !Array.isArray(previousResult.rows) || previousResult.rows.length === 0) {
    return "";
  }

  const rows = previousResult.rows.slice(0, 10);
  const gpName = previousResult.gp_name || "Previous Grand Prix";
  const circuit = previousResult.circuit_name || "";
  const raceDate = previousResult.date || "";
  const sourceUrl = previousResult.source_url || "";

  const items = rows
    .map((row) => {
      const pos = row?.position ?? "—";
      const driver = row?.driver || "Unknown";
      const driverCode = row?.driver_code ? ` <span class="f1-live-mini">(${esc(row.driver_code)})</span>` : "";
      const team = row?.constructor || row?.team || "—";
      return `
        <div class="f1-live-row">
          <div class="f1-live-pos">${esc(pos)}</div>
          <div class="f1-live-driver-wrap">
            <div class="f1-live-driver">${esc(driver)}${driverCode}</div>
            ${row?.grid ? `<div class="f1-live-mini">Grid ${esc(row.grid)}</div>` : ""}
          </div>
          <div class="f1-live-team">${esc(team)}</div>
          <div class="f1-live-result">${buildPreviousResultCell(row)}</div>
        </div>
      `;
    })
    .join("");

  const metaParts = [];
  if (raceDate) metaParts.push(esc(raceDate));
  if (circuit) metaParts.push(esc(circuit));
  const countText = previousResult.rows.length > rows.length ? `Top ${rows.length} of ${previousResult.rows.length}` : `${rows.length} classified`;

  return `
    <section class="f1-empty" style="margin-top: 12px; padding: 0; overflow: hidden;">
      <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; padding: 12px 14px 10px; border-bottom: 1px solid var(--border);">
        <div style="min-width: 0;">
          <div style="font-size: 10px; color: var(--text-muted); letter-spacing: .08em; text-transform: uppercase; font-weight: 800;">上一场结果</div>
          <div style="margin-top: 2px; color: var(--text-main); font-weight: 800; line-height: 1.25;">${esc(cleanGrandPrixName(gpName))}</div>
          <div style="margin-top: 4px; color: var(--text-muted); font-size: 11px; line-height: 1.35;">
            ${metaParts.join(" · ")}${metaParts.length ? " · " : ""}${esc(countText)}
          </div>
        </div>
        ${sourceUrl ? `<a href="${esc(sourceUrl)}" target="_blank" rel="noopener noreferrer" style="font-size: 11px; color: var(--accent-blue); white-space: nowrap; text-decoration: none; font-weight: 700;">Jolpica →</a>` : ""}
      </div>
      <div style="padding-top: 10px;">
        <div class="f1-live-table-head">
          <div>POS</div>
          <div>DRIVER</div>
          <div>TEAM</div>
          <div>RESULT</div>
        </div>
        <div class="f1-live-list">${items}</div>
      </div>
    </section>
  `;
}

function buildBetweenRoundsMarkup(payload) {
  const nextRound = getNextRound(payload);
  const previousRound = getPreviousRound(payload);
  const nextName = nextRound?.name ? cleanGrandPrixName(nextRound.name) : "the next Grand Prix";
  const nextDates = formatRoundDate(nextRound);
  const previousName = previousRound?.name ? cleanGrandPrixName(previousRound.name) : "";
  const previousDates = formatRoundDate(previousRound);

  const nextUrl = nextRound?.flashscore_url || payload?.page_url || "";

  const previousResultMarkup = buildPreviousResultMarkup(payload?.previous_result);

  return `
    <div class="f1-empty">
      <div><strong>当前没有进行中的 F1 session。</strong></div>
      <div>下一站：${esc(nextName)}${nextDates ? ` · ${esc(nextDates)}` : ""}</div>
      ${previousName ? `<div>上一站：${esc(previousName)}${previousDates ? ` · ${esc(previousDates)}` : ""}</div>` : ""}
      ${nextUrl ? `<div style="margin-top: 8px;"><a href="${esc(nextUrl)}" target="_blank" rel="noopener noreferrer">Open Flashscore round page →</a></div>` : ""}
    </div>
    ${previousResultMarkup}
  `;
}

export function renderF1Live(targetEl, payload) {
  if (!targetEl) return;

  if (isBetweenRounds(payload)) {
    targetEl.innerHTML = buildBetweenRoundsMarkup(payload);
    return;
  }

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
