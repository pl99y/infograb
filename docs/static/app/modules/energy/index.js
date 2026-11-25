import { resourceSession } from "./session.js?v=61";
import { clearElement, setEmpty, setError, setLoading } from "../../core/dom.js?v=46";
import { formatNumber, formatSigned } from "../../core/time.js?v=46";
import { renderFreshness } from "../../core/export-meta.js?v=63";

const energyInfoMap = {
  wti: {
    title: "美国 WTI 原油",
    subtitle: "WTI Crude",
    unit: "美元/桶",
    note: "西得克萨斯中间基原油，常被做为反映当前世界石油市场的参考价格",
  },
  brent: {
    title: "布伦特原油",
    subtitle: "Brent Crude",
    unit: "美元/桶",
    note: "产自欧洲北海地区，是全球约三分之二国际原油交易的定价基准",
  },
  murban: {
    title: "穆尔班原油",
    subtitle: "Murban · Platts 现货评估",
    unit: "美元/桶",
    note: "阿布扎比出产的一种高品质原油，常作为中东原油的定价基准之一",
  },
  natural_gas: {
    title: "美国天然气",
    subtitle: "Natural Gas",
    unit: "美元/MMBtu",
    note: "美国出产的以页岩气为主的天然气资源，主要成分为甲烷",
  },
  gasoline: {
    title: "美国汽油",
    subtitle: "Gasoline (RBOB)",
    unit: "美元/加仑",
    note: "一种清洁环保的未完成提炼汽油，美国汽油市场的重要基准价格",
  },
  china: {
    title: "上海原油 SC",
    subtitle: "China Crude Oil Futures",
    unit: "元/桶",
    note: "我国首个国际化的期货品种，代表中国及亚太地区供需关系的价格信号",
  },
};

export function createEnergyModule(ctx) {
  const container = document.getElementById("energy-container");
  const updatedAt = document.getElementById("energyUpdatedAt");
  const tabs = document.getElementById("energyTabs");
  let activeTab = ctx.store?.get("ig_energy_tab", "energy") === "commodities" ? "commodities" : "energy";
  let energy = [], commodities = [];
  let calendar = null;

  function updateSessions() {
    container?.querySelectorAll("[data-resource-key]").forEach(card => {
      const session = resourceSession(card.dataset.resourceKey, calendar);
      card.dataset.sessionState = session.state;
      const badge = card.querySelector(".resource-session");
      if (badge) { badge.textContent = session.label; badge.title = session.detail; }
    });
  }

  function selectTab(value) {
    activeTab = value === "commodities" ? "commodities" : "energy";
    ctx.store?.set("ig_energy_tab", activeTab);
    const heading = document.getElementById("energyPanelTitle");
    if (heading) heading.textContent = "资源行情";
    tabs?.querySelectorAll("button").forEach(button => {
      const active = button.dataset.energyTab === activeTab;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", String(active));
    });
    render(activeTab === "energy" ? energy : commodities);
    updateHeader();
  }

  function getEnergyCardInfo(item) {
    return energyInfoMap[item.quote_key] || {
      title: item.name || item.quote_key || "未知品种",
      subtitle: `${item.symbol || ""} · ${Object.hasOwn(item, "display_price") ? "国际期货折算" : "连续期货"}`,
      unit: item.unit || "",
      note: `期货报价，非现货；${item.contract_name || "具体月份以来源为准"}`,
    };
  }

  function updateHeader() {
    renderFreshness(updatedAt, [activeTab === "energy" ? "energy_latest.json" : "commodities_latest.json"], activeTab === "energy" ? ctx.state.lastEnergyFetchedAt : ctx.state.lastCommodityFetchedAt);
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
      const converted = Object.hasOwn(item, "display_price");
      const displayPrice = converted ? item.display_price : item.price;
      const price = displayPrice == null ? NaN : Number(displayPrice);
      if (converted) info.unit = item.display_unit;
      const change = (item.change == null ? NaN : Number(item.change));
      const changePercent = (item.change_percent == null ? NaN : Number(item.change_percent));
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
      card.dataset.resourceKey = item.quote_key;

      card.innerHTML = `
        <div class="market-symbol">${info.title}</div>
        <div class="market-symbol-sub">${info.subtitle}</div>
        <div class="market-price">
          <div class="market-price-value">${Number.isNaN(price) ? "—" : formatNumber(price)}</div>
          <div class="market-price-unit">${info.unit || item.unit || ""}</div>
        </div>
        ${showChange ? `<div class="market-change ${changeClass}">${changeText}</div>` : ""}
      `;

      const quoteTime = item.timestamp_text || item.fetched_at;
      const quoteDate = new Date(quoteTime);
      const timeText = quoteTime && (Number.isNaN(quoteDate.getTime()) ? String(quoteTime) : quoteDate.toLocaleString());
      const fxNote = converted ? (item.fx
        ? `原价 ${item.price} 美元/金衡盎司；1 美元 = ${item.fx.rate} 人民币；汇率时间 ${new Date(item.fx.quote_time).toLocaleString()}；涨跌幅为美元期货原报价涨跌幅`
        : "缺少有效汇率，暂不显示折算价") : "";
      card.title = [info.note, fxNote, timeText && `报价时间 ${timeText}`].filter(Boolean).join("\n");
      if (converted && item.conversion_status !== "available") {
        const status = document.createElement("div");
        status.className = "market-symbol-sub";
        status.textContent = "汇率暂不可用";
        card.appendChild(status);
      }
      if (item.contract_rolled) {
        const roll = document.createElement("div");
        roll.className = "market-symbol-sub";
        roll.textContent = "合约已换月";
        card.appendChild(roll);
      }
      if (item.status === "unavailable" || item.status === "stale") {
        const status = document.createElement("div");
        status.className = "market-symbol-sub";
        status.textContent = item.status === "unavailable" ? "暂不可用" : "更新失败";
        card.appendChild(status);
      }
      const badge = document.createElement("span");
      badge.className = "resource-session";
      card.appendChild(badge);
      container.appendChild(card);
    });
    updateSessions();
  }

  async function refresh() {
    setLoading(container, "正在加载行情...");
    const results = await Promise.allSettled([
      ctx.api.get("/api/energy/latest"), ctx.api.get("/api/commodities/latest"), ctx.api.get("/api/market/calendar"),
    ]);
    if (results[0].status === "fulfilled") energy = Array.isArray(results[0].value) ? results[0].value : [];
    if (results[1].status === "fulfilled") commodities = Array.isArray(results[1].value) ? results[1].value : [];
    if (results[2].status === "fulfilled") calendar = results[2].value;
    ctx.state.energy = energy;
    const newest = items => items.map(item => item.fetched_at).filter(Boolean).sort().at(-1) || null;
    ctx.state.lastEnergyFetchedAt = newest(energy);
    ctx.state.lastCommodityFetchedAt = newest(commodities);
    selectTab(activeTab);
  }

  return {
    render,
    updateHeader,
    refresh,
    init() {
      setInterval(updateSessions, 30000);
      tabs?.addEventListener("click", event => {
        const button = event.target.closest("[data-energy-tab]");
        if (button) selectTab(button.dataset.energyTab);
      });
    },
  };
}
