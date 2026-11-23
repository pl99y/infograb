import { clearElement, setEmpty, setError, setLoading } from "../../core/dom.js?v=46";
import { renderFreshness } from "../../core/export-meta.js?v=46";
import { marketSession } from "./session.js?v=59";

const marketLabels = {
  nasdaq: { name: "NASDAQ Composite", symbol: "^IXIC" },
  sp500: { name: "S&P 500", symbol: "^GSPC" },
  dowjones: { name: "Dow Jones Industrial Average", symbol: "^DJI" },
  a_share_sse: { name: "上证指数", symbol: "000001" },
  nikkei225: { name: "Nikkei 225", symbol: "^N225" },
  hangseng: { name: "Hang Seng Index", symbol: "^HSI" },
};

function formatSnapshotPrice(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }

  const num = Number(value);
  const abs = Math.abs(num);
  const fractionDigits = abs >= 1000 ? 2 : abs >= 100 ? 2 : 3;

  return num.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  });
}

function formatChangePercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  const n = Number(value);
  return `${n > 0 ? "+" : ""}${n.toFixed(2)}%`;
}

function getDisplayInfo(item) {
  return marketLabels[item?.key] || {
    name: item?.name || "未知指数",
    symbol: item?.symbol || "—",
  };
}

export function createMarketModule(ctx) {
  const container = document.getElementById("market-container");
  const updatedAt = document.getElementById("marketUpdatedAt");
  let calendar = null;

  function updateSessions() {
    container?.querySelectorAll("[data-market-session]").forEach(element => {
      const session = marketSession(element.dataset.marketSession, calendar);
      element.textContent = session.label;
      element.dataset.state = session.state;
      element.title = session.detail;
    });
  }

  function updateHeader() {
    renderFreshness(updatedAt, ["market_latest.json"], ctx.state.lastMarketFetchedAt);
  }

  function render(items) {
    if (!container) return;
    if (!Array.isArray(items) || items.length === 0) {
      setEmpty(container, "暂无市场指数快照。");
      return;
    }

    clearElement(container);

    items.forEach((item) => {
      const info = getDisplayInfo(item);
      const changePercent = Number(item.change_percent);
      let changeClass = "";
      if (!Number.isNaN(changePercent)) {
        changeClass = changePercent >= 0 ? "change-up" : "change-down";
      }

      const row = document.createElement("div");
      row.className = "market-snapshot-row";
      const collected = new Date(item.fetched_at);
      row.title = [item.source, item.quote_time && `报价时间 ${new Date(item.quote_time).toLocaleString()}`,
        item.fetched_at && !Number.isNaN(collected.getTime()) && `采集时间 ${collected.toLocaleString()}`].filter(Boolean).join("\n");
      row.innerHTML = `
        <div class="market-snapshot-meta">
          <div class="market-snapshot-name">${info.name}</div>
          <div class="market-snapshot-symbol">${info.symbol}</div>
          <span class="market-session" data-market-session="${item.key}"></span>
        </div>
        <div class="market-snapshot-values">
          <div class="market-snapshot-price">${formatSnapshotPrice(item?.price)}</div>
          <div class="market-snapshot-change ${changeClass}">${formatChangePercent(item?.change_percent)}</div>
        </div>
      `;
      if (item.status === "unavailable" || item.status === "stale") {
        const status = document.createElement("div");
        status.className = "market-snapshot-symbol";
        status.textContent = item.status === "unavailable" ? "暂不可用" : "更新失败";
        row.querySelector(".market-snapshot-meta").appendChild(status);
      }
      container.appendChild(row);
    });
    updateSessions();
  }

  async function refresh() {
    setLoading(container, "正在加载市场指数...");
    try {
      const results = await Promise.allSettled([
        ctx.api.get("/api/market-snapshots/latest"), ctx.api.get("/api/market/calendar"),
      ]);
      if (results[1].status === "fulfilled") calendar = results[1].value;
      if (results[0].status !== "fulfilled") throw results[0].reason;
      const data = results[0].value;
      ctx.state.marketSnapshots = Array.isArray(data) ? data : [];
      render(ctx.state.marketSnapshots);

      const timestamps = ctx.state.marketSnapshots
        .map((item) => item?.fetched_at)
        .filter(Boolean)
        .map((value) => new Date(value).getTime())
        .filter((value) => !Number.isNaN(value));

      const fallbackUpdatedAt = timestamps.length > 0
        ? new Date(Math.max(...timestamps)).toISOString()
        : null;
      ctx.state.lastMarketFetchedAt = fallbackUpdatedAt;
      updateHeader();
    } catch (error) {
      console.error("Failed to load market snapshots:", error);
      ctx.state.lastMarketFetchedAt = null;
      updateHeader();
      setError(container, "市场指数加载失败。");
    }
  }

  return {
    render,
    updateHeader,
    refresh,
    init() {
      setInterval(updateSessions, 30000);
    },
  };
}
