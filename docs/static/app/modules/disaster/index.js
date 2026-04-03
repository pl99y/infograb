import { setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { renderWeatherPane } from "./render-weather.js";
import { renderDisasterPane } from "./render-disaster.js?v=37";

const DISASTER_TAB_KEY = "ig_disaster_tab";

function normalizeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeWeather(items) {
  return normalizeArray(items)
    .filter((item) => item && item.title)
    .map((item) => ({
      ...item,
      event_type: item.event_type || "weather",
      location_text: item.location_text || item.summary || item.payload?.areaDesc || "",
      severity_level:
        item.severity_level ||
        item.color_level ||
        item.payload?.severity_label ||
        item.payload?.alert_level ||
        item.alert_level ||
        "",
      published_at: item.published_at || item.issued_at || item.effective_at || item.fetched_at,
      updated_at: item.updated_at || item.fetched_at || item.issued_at || item.effective_at,
      expires_at: item.expires_at || item.payload?.expires || "",
      link_url: "https://severeweather.wmo.int/list.html",
    }));
}

function sortByPrimaryTimeDesc(items) {
  return [...normalizeArray(items)].sort((a, b) => {
    const ta = new Date(a?.primary_time || 0).getTime();
    const tb = new Date(b?.primary_time || 0).getTime();
    if (!Number.isNaN(tb - ta) && tb !== ta) return tb - ta;
    const sa = Number(a?.severity_rank ?? 99);
    const sb = Number(b?.severity_rank ?? 99);
    return sa - sb;
  });
}

function severityRank(level) {
  const value = String(level || "").trim().toLowerCase();
  if (["critical", "extreme", "red"].includes(value)) return 0;
  if (["high"].includes(value)) return 1;
  if (["orange", "moderate", "medium", "elevated"].includes(value)) return 2;
  if (["green", "low"].includes(value)) return 3;
  return 4;
}

function normalizeDisasters(items) {
  const allowed = new Set(["earthquake", "tsunami", "volcano", "typhoon", "flood"]);
  return sortByPrimaryTimeDesc(
    normalizeArray(items)
      .filter((item) => item && allowed.has(String(item.event_type || "").toLowerCase()))
      .map((item) => {
        const primaryTime = item.occurred_at || item.started_at || item.published_at || item.updated_at || item.fetched_at || "";
        return {
          ...item,
          summary: item.summary || item.location_text || "",
          primary_time: primaryTime,
          display_time: primaryTime,
          severity_rank: severityRank(item.severity_level),
        };
      }),
  );
}

function pickLatestTimestamp(items) {
  const values = normalizeArray(items)
    .map((item) => item.fetched_at || item.updated_at || item.issued_at || item.effective_at || item.primary_time || item.occurred_at)
    .filter(Boolean)
    .map((value) => new Date(value).getTime())
    .filter((value) => !Number.isNaN(value));

  if (!values.length) return null;
  return new Date(Math.max(...values)).toISOString();
}

export function createDisasterModule(ctx) {
  const instantContainer = document.getElementById("disaster-instant-container");
  const ongoingPanel = document.getElementById("disasterOngoingPanel");
  const updatedAt = document.getElementById("disasterUpdatedAt");
  const refreshBtn = document.getElementById("disasterRefreshBtn");
  const tabs = document.getElementById("disasterTabs");
  const instantPanel = document.getElementById("disasterInstantPanel");

  function getTabState() {
    if (ctx.state.disasterTab) return ctx.state.disasterTab;
    const saved = ctx.store?.get?.(DISASTER_TAB_KEY, "instant");
    ctx.state.disasterTab = saved || "instant";
    return ctx.state.disasterTab;
  }

  function setTabState(value) {
    ctx.state.disasterTab = value || "instant";
    ctx.store?.set?.(DISASTER_TAB_KEY, ctx.state.disasterTab);
  }

  function applyTab() {
    const activeTab = getTabState();
    tabs?.querySelectorAll(".disaster-tab-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === activeTab);
    });
    instantPanel?.classList.toggle("active", activeTab === "instant");
    ongoingPanel?.classList.toggle("active", activeTab === "ongoing");
  }

  function updateHeader() {
    if (!updatedAt) return;
    const value = ctx.state.lastDisasterFetchedAt;
    if (!value) {
      updatedAt.textContent = "（暂无更新时间）";
      updatedAt.title = "";
      return;
    }
    updatedAt.textContent = `（${formatRelativeLocalTime(value)}更新）`;
    updatedAt.title = formatAbsoluteLocalDateTime(value);
  }

  function renderInstant() {
    if (!instantContainer) return;
    instantContainer.innerHTML = renderWeatherPane(normalizeWeather(ctx.state.weatherAlerts));
  }

  function renderOngoing() {
    if (!ongoingPanel) return;
    const host = ongoingPanel.querySelector(".disaster-list");
    if (!host) return;
    host.innerHTML = renderDisasterPane(normalizeDisasters(ctx.state.disasterOngoing));
  }

  async function refresh() {
    if (instantContainer) setLoading(instantContainer, "正在加载即时警报...");
    const host = ongoingPanel?.querySelector(".disaster-list");
    if (host) setLoading(host, "正在加载自然灾害...");

    try {
      const [weatherItems, ongoingItems] = await Promise.all([
        ctx.api.get("/api/weather/alerts?limit=60"),
        ctx.api.get("/api/disaster/ongoing?limit=80"),
      ]);

      ctx.state.weatherAlerts = normalizeWeather(weatherItems);
      ctx.state.disasterOngoing = normalizeDisasters(ongoingItems);
      ctx.state.disasterInstant = ctx.state.disasterOngoing;

      renderInstant();
      renderOngoing();

      ctx.state.lastDisasterFetchedAt = pickLatestTimestamp([
        ...ctx.state.weatherAlerts,
        ...ctx.state.disasterOngoing,
      ]);
      updateHeader();
    } catch (error) {
      console.error("Failed to load disaster module:", error);
      ctx.state.lastDisasterFetchedAt = null;
      updateHeader();
      if (instantContainer) {
        setError(instantContainer, "灾害数据加载失败。先检查 /api/weather/alerts 和 /api/disaster/ongoing。");
      }
      if (host) {
        host.innerHTML = `
          <div class="disaster-pane-empty">
            <div class="disaster-pane-empty-title">自然灾害加载失败</div>
            <div class="disaster-pane-empty-subtitle">请确认后端接口返回正常。</div>
          </div>
        `;
      }
    }
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";
    try {
      const result = await ctx.api.post("/api/disaster/refresh");
      const weatherErrors = Array.isArray(result?.weather?.errors) ? result.weather.errors : [];
      const disasterErrors = Array.isArray(result?.disaster?.errors) ? result.disaster.errors : [];
      await refresh();
      if (weatherErrors.length || disasterErrors.length) {
        console.warn("Natural hazards refresh completed with errors:", result);
        alert(`刷新已执行，但有部分源失败。
天气错误：${weatherErrors.length}
灾害错误：${disasterErrors.length}`);
      }
    } catch (error) {
      console.error("Manual disaster refresh failed:", error);
      alert("灾害模块手动刷新失败。请检查 /api/disaster/refresh、/api/weather/alerts 和 /api/disaster/ongoing。");
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      if (label) label.textContent = "刷新";
    }
  }

  return {
    refresh,
    updateHeader,
    init() {
      tabs?.addEventListener("click", (event) => {
        const button = event.target.closest(".disaster-tab-btn");
        if (!button) return;
        const nextTab = button.dataset.tab;
        if (!nextTab) return;
        setTabState(nextTab);
        applyTab();
      });

      refreshBtn?.addEventListener("click", manualRefresh);
      ongoingPanel?.addEventListener("click", (event) => {
        const mapLink = event.target.closest("[data-hazard-map-link]");
        if (mapLink) return;

        const card = event.target.closest(".hazard-card-disaster[data-card-url]");
        if (!card) return;
        if (event.target.closest("a, button, input, textarea, select")) return;

        const href = card.dataset.cardUrl;
        if (!href) return;
        window.open(href, "_blank", "noopener,noreferrer");
      });

      ongoingPanel?.addEventListener("keydown", (event) => {
        const card = event.target.closest(".hazard-card-disaster[data-card-url]");
        if (!card) return;
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        const href = card.dataset.cardUrl;
        if (!href) return;
        window.open(href, "_blank", "noopener,noreferrer");
      });

      applyTab();
      renderInstant();
      renderOngoing();
    },
  };
}

export default createDisasterModule;
