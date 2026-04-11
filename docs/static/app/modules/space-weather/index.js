import { clearElement, setEmpty, setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { getExportProfileGeneratedAt } from "../../core/export-meta.js";

const DEFAULT_DAY_LABELS = ["第 1 天", "第 2 天", "第 3 天"];

const MESSAGE_TYPE_LABELS = {
  alert: "告警",
  continued_alert: "持续告警",
  warning: "预警",
  extended_warning: "延长预警",
  watch: "监视",
  summary: "摘要",
  cancel_alert: "取消告警",
  cancel_warning: "取消预警",
  cancel_watch: "取消监视",
  cancel_summary: "取消摘要",
  notice: "通知",
};

const TAB_LABELS = {
  launches: "发射动态",
  alerts: "NOAA Alerts",
  forecast: "3-Day Forecast",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeDayLabels(panel) {
  const days = panel?.geomagnetic?.days;
  if (Array.isArray(days) && days.length >= 3) return days.slice(0, 3);
  const solarDays = panel?.solar_radiation?.days;
  if (Array.isArray(solarDays) && solarDays.length >= 3) return solarDays.slice(0, 3);
  const radioDays = panel?.radio_blackout?.days;
  if (Array.isArray(radioDays) && radioDays.length >= 3) return radioDays.slice(0, 3);
  return DEFAULT_DAY_LABELS;
}

function formatMessageType(messageType) {
  return MESSAGE_TYPE_LABELS[messageType] || "消息";
}

function compactSingleLine(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function formatIssueTime(value) {
  if (!value) return "—";
  return formatAbsoluteLocalDateTime(value);
}

function hasLaunchValue(value) {
  const text = compactSingleLine(value);
  if (!text) return false;
  const lowered = text.toLowerCase();
  return text !== "—" && lowered !== "null" && lowered !== "undefined";
}

function formatLaunchValue(value) {
  return hasLaunchValue(value) ? compactSingleLine(value) : "—";
}

function formatLaunchDateParts(value) {
  const text = compactSingleLine(value);
  if (!text) return { month: "--", day: "--" };

  const match = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) {
    return { month: match[2], day: match[3] };
  }

  const date = new Date(text);
  if (!Number.isNaN(date.getTime())) {
    return {
      month: String(date.getMonth() + 1).padStart(2, "0"),
      day: String(date.getDate()).padStart(2, "0"),
    };
  }

  return { month: "--", day: text.slice(-2).padStart(2, "0") };
}

function normalizeOutcome(value) {
  const text = compactSingleLine(value);
  if (!text) return null;

  const lowered = text.toLowerCase();
  if (["success", "successful", "succeeded", "成功"].includes(lowered)) {
    return { label: "成功", tone: "success" };
  }
  if (["failure", "failed", "fail", "失败"].includes(lowered)) {
    return { label: "失败", tone: "failure" };
  }
  if (
    [
      "partial success",
      "partially successful",
      "partial failure",
      "partially failed",
      "部分成功",
      "部分失败",
    ].includes(lowered)
  ) {
    return { label: "部分成功", tone: "partial" };
  }
  return { label: text, tone: "neutral" };
}

function cleanPayloadLine(value) {
  if (!hasLaunchValue(value)) return "";
  return compactSingleLine(value).replace(/^载荷\/运力\s*[·:：-]\s*/u, "");
}

function translateLaunchCategory(value) {
  const text = compactSingleLine(value);
  if (!text) return "";
  const lowered = text.toLowerCase();
  if (lowered === "orbital") return "轨道";
  if (lowered === "deep space") return "深空";
  if (lowered === "suborbital") return "亚轨道";
  return text;
}

function parseLaunchMetrics(value) {
  const text = cleanPayloadLine(value);
  if (!text) return [];

  const parts = text
    .split(/\s*[·•|｜]\s*/u)
    .map((part) => compactSingleLine(part))
    .filter(Boolean);

  const metrics = [];
  for (const part of parts) {
    const match = part.match(/^(载荷|payload|LEO|SSO|近地轨道|太阳同步)\s*(.+)$/i);
    if (!match) continue;
    const rawLabel = compactSingleLine(match[1]);
    const rawValue = compactSingleLine(match[2]).replace(/\s+t$/i, "t");
    const lowered = rawLabel.toLowerCase();
    let label = rawLabel;
    if (rawLabel === "载荷" || lowered === "payload") label = "载荷";
    else if (lowered === "leo" || rawLabel === "近地轨道") label = "近地轨道";
    else if (lowered === "sso" || rawLabel === "太阳同步") label = "太阳同步";
    metrics.push({ label, value: rawValue });
  }
  return metrics;
}

function renderLaunchMetricTag(metric) {
  if (!metric?.label || !metric?.value) return "";
  return `
    <span class="space-launch-metric-tag">
      <span class="space-launch-metric-tag-label">${escapeHtml(metric.label)}</span>
      <span class="space-launch-metric-tag-value">${escapeHtml(metric.value)}</span>
    </span>
  `;
}

function pickLatestTimestamp(alerts, forecast, launches) {
  const values = [];

  for (const item of Array.isArray(alerts) ? alerts : []) {
    if (item?.issue_datetime) values.push(new Date(item.issue_datetime).getTime());
    if (item?.fetched_at) values.push(new Date(item.fetched_at).getTime());
  }

  for (const key of ["fetched_at", "forecast_issued_at", "geomag_issued_at"]) {
    if (forecast?.[key]) values.push(new Date(forecast[key]).getTime());
  }

  if (launches?.fetched_at) values.push(new Date(launches.fetched_at).getTime());

  const valid = values.filter((value) => !Number.isNaN(value));
  if (!valid.length) return null;
  return new Date(Math.max(...valid)).toISOString();
}

function renderProbabilityTable(days, probabilities) {
  const rows = [
    ["活跃", probabilities?.active || []],
    ["小风暴", probabilities?.minor_storm || []],
    ["中等风暴", probabilities?.moderate_storm || []],
    ["强/极强", probabilities?.strong_extreme_storm || []],
  ];

  return `
    <div class="space-weather-prob-table-wrap">
      <table class="space-weather-prob-table">
        <thead>
          <tr>
            <th>级别</th>
            ${days.map((day) => `<th>${escapeHtml(day)}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
          ${rows.map(([label, values]) => `
            <tr>
              <td>${escapeHtml(label)}</td>
              ${days.map((_, index) => `<td>${escapeHtml(values[index] ?? "—")}%</td>`).join("")}
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderMiniMetricGrid(days, label, values) {
  return `
    <div class="space-weather-mini-block">
      <div class="space-weather-mini-label">${escapeHtml(label)}</div>
      <div class="space-weather-mini-grid">
        ${days.map((day, index) => `
          <div class="space-weather-mini-cell">
            <div class="space-weather-mini-day">${escapeHtml(day)}</div>
            <div class="space-weather-mini-value">${escapeHtml(values?.[index] ?? "—")}</div>
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function renderAlerts(container, alerts) {
  if (!container) return;
  if (!Array.isArray(alerts) || !alerts.length) {
    setEmpty(container, "最近 5 天内暂无 NOAA 告警。");
    return;
  }

  clearElement(container);

  alerts.forEach((item) => {
    const card = document.createElement("article");
    card.className = "space-weather-alert-card";

    const impacts = compactSingleLine(item?.impacts_text || "");
    const description = compactSingleLine(item?.description_text || "");
    const summary = impacts || description || "暂无额外影响说明。";

    card.innerHTML = `
      <div class="space-weather-alert-top">
        <div class="space-weather-alert-badges">
          <span class="space-weather-badge">${escapeHtml(formatMessageType(item?.message_type))}</span>
          ${item?.noaa_scale ? `<span class="space-weather-badge space-weather-badge-scale">${escapeHtml(item.noaa_scale)}</span>` : ""}
          ${item?.product_id ? `<span class="space-weather-badge space-weather-badge-muted">${escapeHtml(item.product_id)}</span>` : ""}
        </div>
        <div class="space-weather-alert-time" title="${escapeHtml(formatIssueTime(item?.issue_datetime))}">${escapeHtml(formatRelativeLocalTime(item?.issue_datetime))}</div>
      </div>
      <div class="space-weather-alert-title">${escapeHtml(item?.headline || "未命名消息")}</div>
      <div class="space-weather-alert-summary">${escapeHtml(summary)}</div>
    `;

    container.appendChild(card);
  });
}

function renderForecast(container, forecastPayload) {
  if (!container) return;

  const record = forecastPayload?.panel || forecastPayload;
  if (!record || typeof record !== "object") {
    setEmpty(container, "暂无三日展望数据。");
    return;
  }

  const days = normalizeDayLabels(record);
  const geomagnetic = record?.geomagnetic || {};
  const solar = record?.solar_radiation || {};
  const radio = record?.radio_blackout || {};

  container.innerHTML = `
    <section class="space-weather-forecast-card">
      <div class="space-weather-forecast-card-title">地磁活动</div>
      <div class="space-weather-forecast-copy">${escapeHtml(geomagnetic?.observed_summary || "")}</div>
      <div class="space-weather-forecast-copy">${escapeHtml(geomagnetic?.expected_summary || "")}</div>
      ${renderProbabilityTable(days, geomagnetic?.probabilities || {})}
      <div class="space-weather-rationale"><span>依据</span>${escapeHtml(geomagnetic?.rationale || "—")}</div>
    </section>

    <section class="space-weather-forecast-card">
      <div class="space-weather-forecast-card-title">太阳辐射风暴</div>
      <div class="space-weather-forecast-copy">${escapeHtml(solar?.observed_summary || "")}</div>
      ${renderMiniMetricGrid(days, "S1 及以上", solar?.s1_or_greater || [])}
      <div class="space-weather-rationale"><span>依据</span>${escapeHtml(solar?.rationale || "—")}</div>
    </section>

    <section class="space-weather-forecast-card">
      <div class="space-weather-forecast-card-title">无线电黑障</div>
      <div class="space-weather-forecast-copy">${escapeHtml(radio?.observed_summary || "")}</div>
      ${renderMiniMetricGrid(days, "R1–R2", radio?.r1_r2 || [])}
      ${renderMiniMetricGrid(days, "R3 及以上", radio?.r3_or_greater || [])}
      <div class="space-weather-rationale"><span>依据</span>${escapeHtml(radio?.rationale || "—")}</div>
    </section>
  `;
}

function renderLaunchMetaChip(text, tone = "default") {
  if (!hasLaunchValue(text)) return "";
  const toneClass = tone ? ` space-launch-meta-chip-${tone}` : "";
  return `<span class="space-launch-meta-chip${toneClass}">${escapeHtml(compactSingleLine(text))}</span>`;
}

function renderLaunches(container, payload) {
  if (!container) return;
  const items = Array.isArray(payload?.items) ? payload.items : [];
  if (!items.length) {
    setEmpty(container, "暂无发射动态数据。");
    return;
  }

  clearElement(container);

  items.forEach((item) => {
    const card = document.createElement("article");
    card.className = "space-launch-row";

    const dateParts = formatLaunchDateParts(item?.date);
    const vehicle = hasLaunchValue(item?.vehicle) ? compactSingleLine(item.vehicle) : "型号待补";
    const site = formatLaunchValue(item?.launch_site);
    const outcome = normalizeOutcome(item?.outcome);
    const country = hasLaunchValue(item?.country) ? compactSingleLine(item.country) : "国家未知";
    const category = translateLaunchCategory(item?.category);
    const metrics = parseLaunchMetrics(item?.actual_payload_capacity);
    const starlink = compactSingleLine(item?.starlink_mission || "");
    const showStarlink = /^(yes|true|是|星链|starlink)$/i.test(starlink);

    card.innerHTML = `
      <div class="space-launch-row-aside">
        <div class="space-launch-date-card">
          <div class="space-launch-date-month">${escapeHtml(dateParts.month)}月</div>
          <div class="space-launch-date-day">${escapeHtml(dateParts.day)}</div>
        </div>
      </div>
      <div class="space-launch-row-main">
        <div class="space-launch-row-head">
          <div class="space-launch-row-title">${escapeHtml(vehicle)}</div>
          ${outcome ? `<span class="space-launch-outcome-badge space-launch-outcome-${escapeHtml(outcome.tone)}">${escapeHtml(outcome.label)}</span>` : ""}
        </div>
        <div class="space-launch-row-site">${escapeHtml(site)}</div>
        <div class="space-launch-row-meta">
          ${renderLaunchMetaChip(country, "country")}
          ${renderLaunchMetaChip(category, "category")}
          ${showStarlink ? '<span class="space-launch-meta-chip space-launch-meta-chip-starlink">Starlink</span>' : ""}
        </div>
        ${metrics.length ? `
          <div class="space-launch-row-metrics">
            ${metrics.map(renderLaunchMetricTag).join("")}
          </div>
        ` : ""}
      </div>
    `;
    container.appendChild(card);
  });
}

export function createSpaceWeatherModule(ctx) {
  const launchesContainer = document.getElementById("spaceLaunchesContainer");
  const alertsContainer = document.getElementById("spaceWeatherAlertsContainer");
  const forecastContainer = document.getElementById("spaceWeatherForecastContainer");
  const updatedAt = document.getElementById("spaceWeatherUpdatedAt");
  const refreshBtn = document.getElementById("spaceWeatherRefreshBtn");
  const tabButtons = Array.from(document.querySelectorAll("[data-space-weather-tab]"));
  const tabPanels = Array.from(document.querySelectorAll("[data-space-weather-panel]"));

  let activeTab = tabButtons.find((button) => button.classList.contains("active"))?.getAttribute("data-space-weather-tab") || "launches";

  function setActiveTab(tabName) {
    activeTab = tabName;

    tabButtons.forEach((button) => {
      const isActive = button.getAttribute("data-space-weather-tab") === tabName;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    tabPanels.forEach((panel) => {
      const isActive = panel.getAttribute("data-space-weather-panel") === tabName;
      panel.classList.toggle("active", isActive);
      panel.hidden = !isActive;
    });
  }

  function bindTabs() {
    if (!tabButtons.length || !tabPanels.length) return;

    tabButtons.forEach((button) => {
      const tabName = button.getAttribute("data-space-weather-tab") || "launches";
      if (!button.textContent?.trim()) button.textContent = TAB_LABELS[tabName] || tabName;
      button.addEventListener("click", () => setActiveTab(tabName));
    });

    setActiveTab(activeTab);
  }

  function updateHeader() {
    if (!updatedAt) return;
    if (!ctx.state.lastSpaceWeatherFetchedAt) {
      updatedAt.textContent = "（暂无更新时间）";
      updatedAt.title = "";
      return;
    }
    updatedAt.textContent = `（${formatRelativeLocalTime(ctx.state.lastSpaceWeatherFetchedAt)}更新）`;
    updatedAt.title = formatAbsoluteLocalDateTime(ctx.state.lastSpaceWeatherFetchedAt);
  }

  async function refresh() {
    if (launchesContainer) setLoading(launchesContainer, "正在加载发射动态...");
    if (alertsContainer) setLoading(alertsContainer, "正在加载 NOAA Alerts...");
    if (forecastContainer) setLoading(forecastContainer, "正在加载 3-Day Forecast...");

    try {
      const [launches, alerts, forecast] = await Promise.all([
        ctx.api.get("/api/space-weather/launches"),
        ctx.api.get("/api/space-weather/alerts?limit=30"),
        ctx.api.get("/api/space-weather/forecast"),
      ]);

      ctx.state.spaceLaunches = launches || { items: [] };
      ctx.state.spaceWeatherAlerts = Array.isArray(alerts) ? alerts : [];
      ctx.state.spaceWeatherForecast = forecast || null;
      const exportGeneratedAt = await getExportProfileGeneratedAt("12h");
      ctx.state.lastSpaceWeatherFetchedAt = exportGeneratedAt || pickLatestTimestamp(ctx.state.spaceWeatherAlerts, ctx.state.spaceWeatherForecast, ctx.state.spaceLaunches);
      renderLaunches(launchesContainer, ctx.state.spaceLaunches);
      renderAlerts(alertsContainer, ctx.state.spaceWeatherAlerts);
      renderForecast(forecastContainer, ctx.state.spaceWeatherForecast);
      updateHeader();
    } catch (error) {
      console.error("Failed to load space weather data:", error);
      ctx.state.lastSpaceWeatherFetchedAt = null;
      updateHeader();
      setError(launchesContainer, "发射动态加载失败。");
      setError(alertsContainer, "NOAA Alerts 加载失败。");
      setError(forecastContainer, "NOAA Forecast 加载失败。");
    }
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";
    try {
      await ctx.api.post("/api/space-weather/refresh");
      await refresh();
    } catch (error) {
      console.error("Manual space weather refresh failed:", error);
      alert("空间监控模块手动刷新失败。");
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      if (label) label.textContent = "刷新";
    }
  }

  return {
    refresh,
    updateHeader,
    init() {
      bindTabs();
      refreshBtn?.addEventListener("click", manualRefresh);
    },
  };
}
