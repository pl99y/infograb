import { clearElement, setEmpty, setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatNumber, formatRelativeLocalTime, formatSigned } from "../../core/time.js";

const energyInfoMap = {
  wti: {
    title: "美国 WTI 原油",
    subtitle: "WTI Crude",
    unit: "美元/桶",
    note: "改为 CME 官方页面抓取。",
  },
  brent: {
    title: "布伦特原油",
    subtitle: "Brent Crude",
    unit: "美元/桶",
    note: "改为 CME 官方页面抓取。",
  },
  murban: {
    title: "穆尔班原油",
    subtitle: "Murban Crude",
    unit: "美元/桶",
    note: "当前仍保留原来源。",
  },
  natural_gas: {
    title: "美国天然气",
    subtitle: "Natural Gas",
    unit: "美元/MMBtu",
    note: "改为 CME 官方页面抓取。",
  },
  gasoline: {
    title: "美国汽油",
    subtitle: "Gasoline (RBOB)",
    unit: "美元/加仑",
    note: "改为 CME 官方页面抓取。",
  },
  china: {
    title: "上海原油 SC",
    subtitle: "China Crude Oil Futures",
    unit: "元/桶",
    note: "",
  },
};

export function createEnergyModule(ctx) {
  const container = document.getElementById("energy-container");
  const updatedAt = document.getElementById("energyUpdatedAt");
  const refreshBtn = document.getElementById("energyRefreshBtn");

  function getEnergyCardInfo(item) {
    return energyInfoMap[item.quote_key] || {
      title: item.name || item.quote_key || "未知品种",
      subtitle: item.name || "",
      unit: item.unit || "",
      note: "",
    };
  }

  function updateHeader() {
    if (!updatedAt) return;
    if (!ctx.state.lastEnergyFetchedAt) {
      updatedAt.textContent = "（暂无更新时间）";
      updatedAt.title = "";
      return;
    }
    updatedAt.textContent = `（${formatRelativeLocalTime(ctx.state.lastEnergyFetchedAt)}更新）`;
    updatedAt.title = formatAbsoluteLocalDateTime(ctx.state.lastEnergyFetchedAt);
  }

  function render(items) {
    if (!container) return;
    if (!Array.isArray(items) || items.length === 0) {
      setEmpty(container, "暂无能源数据。");
      return;
    }

    clearElement(container);

    items.forEach((item) => {
      const info = getEnergyCardInfo(item);
      const price = Number(item.price);
      const change = Number(item.change);
      const changePercent = Number(item.change_percent);
      const showChange = true;

      let changeClass = "";
      if (!Number.isNaN(changePercent)) {
        changeClass = changePercent >= 0 ? "change-up" : "change-down";
      } else if (!Number.isNaN(change) && showChange) {
        changeClass = change >= 0 ? "change-up" : "change-down";
      }

      let changeText = "—";
      if (!Number.isNaN(changePercent)) {
        changeText = formatSigned(changePercent, "%");
      } else if (!Number.isNaN(change)) {
        changeText = formatSigned(change);
      }

      const card = document.createElement("div");
      card.className = "market-card";
      if (info.note) card.title = info.note;

      card.innerHTML = `
        <div class="market-symbol">${info.title}</div>
        <div class="market-symbol-sub">${info.subtitle}</div>
        <div class="market-price">
          <div class="market-price-value">${Number.isNaN(price) ? "—" : formatNumber(price)}</div>
          <div class="market-price-unit">${info.unit || item.unit || ""}</div>
        </div>
        ${showChange ? `<div class="market-change ${changeClass}">${changeText}</div>` : ""}
      `;

      container.appendChild(card);
    });
  }

  async function refresh() {
    setLoading(container, "正在加载能源数据...");
    try {
      const data = await ctx.api.get("/api/energy/latest");
      ctx.state.energy = Array.isArray(data) ? data : [];
      render(ctx.state.energy);

      const timestamps = ctx.state.energy
        .map((item) => item.fetched_at)
        .filter(Boolean)
        .map((value) => new Date(value).getTime())
        .filter((value) => !Number.isNaN(value));

      ctx.state.lastEnergyFetchedAt = timestamps.length > 0 ? new Date(Math.max(...timestamps)).toISOString() : null;
      updateHeader();
    } catch (error) {
      console.error("Failed to load energy data:", error);
      ctx.state.lastEnergyFetchedAt = null;
      updateHeader();
      setError(container, "能源数据加载失败。");
    }
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";
    try {
      await ctx.api.post("/api/energy/refresh");
      await refresh();
    } catch (error) {
      console.error("Manual energy refresh failed:", error);
      alert("能源模块手动刷新失败。");
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      if (label) label.textContent = "刷新";
    }
  }

  return {
    render,
    updateHeader,
    refresh,
    init() {
      refreshBtn?.addEventListener("click", manualRefresh);
    },
  };
}
