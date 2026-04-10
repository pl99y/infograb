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
  alerts: "NOAA Alerts",
  forecast: "3-Day Forecast",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeDayLabels(panel) {
  const days = panel?.geomagnetic?.days;
  if (Array.isArray(days) && days.length >= 3) {
    return days.slice(0, 3);
  }
  const solarDays = panel?.solar_radiation?.days;
  if (Array.isArray(solarDays) && solarDays.length >= 3) {
    return solarDays.slice(0, 3);
  }
  const radioDays = panel?.radio_blackout?.days;
  if (Array.isArray(radioDays) && radioDays.length >= 3) {
    return radioDays.slice(0, 3);
  }
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

function pickLatestTimestamp(alerts, forecast) {
  const values = [];

  for (const item of Array.isArray(alerts) ? alerts : []) {
    if (item?.issue_datetime) values.push(new Date(item.issue_datetime).getTime());
    if (item?.fetched_at) values.push(new Date(item.fetched_at).getTime());
  }

  for (const key of ["fetched_at", "forecast_issued_at", "geomag_issued_at"]) {
    if (forecast?.[key]) values.push(new Date(forecast[key]).getTime());
  }

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

export function createSpaceWeatherModule(ctx) {
  const alertsContainer = document.getElementById("spaceWeatherAlertsContainer");
  const forecastContainer = document.getElementById("spaceWeatherForecastContainer");
  const updatedAt = document.getElementById("spaceWeatherUpdatedAt");
  const refreshBtn = document.getElementById("spaceWeatherRefreshBtn");
  const tabButtons = Array.from(document.querySelectorAll("[data-space-weather-tab]"));
  const tabPanels = Array.from(document.querySelectorAll("[data-space-weather-panel]"));

  let activeTab = "alerts";

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
      const tabName = button.getAttribute("data-space-weather-tab") || "alerts";
      if (!button.textContent?.trim()) {
        button.textContent = TAB_LABELS[tabName] || tabName;
      }
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
    if (alertsContainer) setLoading(alertsContainer, "正在加载 NOAA Alerts...");
    if (forecastContainer) setLoading(forecastContainer, "正在加载 3-Day Forecast...");

    try {
      const [alerts, forecast] = await Promise.all([
        ctx.api.get("/api/space-weather/alerts?limit=30"),
        ctx.api.get("/api/space-weather/forecast"),
      ]);

      ctx.state.spaceWeatherAlerts = Array.isArray(alerts) ? alerts : [];
      ctx.state.spaceWeatherForecast = forecast || null;
      const exportGeneratedAt = await getExportProfileGeneratedAt("12h");
      ctx.state.lastSpaceWeatherFetchedAt = exportGeneratedAt || pickLatestTimestamp(ctx.state.spaceWeatherAlerts, ctx.state.spaceWeatherForecast);

      renderAlerts(alertsContainer, ctx.state.spaceWeatherAlerts);
      renderForecast(forecastContainer, ctx.state.spaceWeatherForecast);
      updateHeader();
    } catch (error) {
      console.error("Failed to load space weather data:", error);
      ctx.state.lastSpaceWeatherFetchedAt = null;
      updateHeader();
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
      alert("太空天气模块手动刷新失败。");
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
