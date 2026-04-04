import { clearElement, setEmpty, setError, setLoading } from "../../core/dom.js";
import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatTodayFactor(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  return Number(value).toFixed(3);
}

function renderTable(items) {
  const head = `
    <thead>
      <tr>
        <th>#</th>
        <th>饰品名称</th>
        <th>日成交量</th>
        <th>最低售价</th>
        <th>最优寄售</th>
        <th>最优求购</th>
        <th>稳定求购</th>
        <th>近期成交</th>
        <th>交易平台</th>
        <th>更新时间</th>
      </tr>
    </thead>
  `;

  const body = items.map((item) => `
    <tr>
      <td>${escapeHtml(item?.rank ?? "")}</td>
      <td class="iflow-item-name" title="${escapeHtml(item?.item_name ?? "")}">${escapeHtml(item?.item_name ?? "")}</td>
      <td>${escapeHtml(item?.daily_volume ?? "")}</td>
      <td>${escapeHtml(item?.min_price ?? "")}</td>
      <td>${escapeHtml(item?.best_sell_ratio ?? "")}</td>
      <td>${escapeHtml(item?.best_buy_ratio ?? "")}</td>
      <td>${escapeHtml(item?.safe_buy_ratio ?? "")}</td>
      <td>${escapeHtml(item?.recent_ratio ?? "")}</td>
      <td>${escapeHtml(item?.platform ?? "")}</td>
      <td>${escapeHtml(item?.updated_text ?? "")}</td>
    </tr>
  `).join("");

  return `
    <div class="iflow-table-scroll">
      <table class="iflow-table">
        ${head}
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

export function createIflowModule(ctx) {
  const container = document.getElementById("iflow-container");
  const updatedAt = document.getElementById("iflowUpdatedAt");
  const todayFactorEl = document.getElementById("iflowTodayFactor");
  const refreshBtn = document.getElementById("iflowRefreshBtn");

  function updateHeader() {
    if (!updatedAt) return;
    if (!ctx.state.lastIflowFetchedAt) {
      updatedAt.textContent = "暂无更新";
      updatedAt.title = "";
      return;
    }
    updatedAt.textContent = `${formatRelativeLocalTime(ctx.state.lastIflowFetchedAt)}更新`;
    updatedAt.title = formatAbsoluteLocalDateTime(ctx.state.lastIflowFetchedAt);
  }

  function render(payload) {
    if (!container) return;
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    if (!rows.length) {
      setEmpty(container, "暂无道爷频倒数据。");
      return;
    }

    clearElement(container);

    if (todayFactorEl) {
      todayFactorEl.textContent = `今日指数：${formatTodayFactor(payload?.today_factor)}`;
    }

    const tableWrap = document.createElement("div");
    tableWrap.innerHTML = renderTable(rows.slice(0, 10));
    container.appendChild(tableWrap.firstElementChild);
  }

  async function refresh() {
    setLoading(container, "正在加载道爷频倒...");
    try {
      const data = await ctx.api.get("/api/iflow/latest");
      ctx.state.iflowData = data || { rows: [] };
      render(ctx.state.iflowData);
      ctx.state.lastIflowFetchedAt = data?.fetched_at || null;
      updateHeader();
    } catch (error) {
      console.error("Failed to load iflow data:", error);
      ctx.state.lastIflowFetchedAt = null;
      updateHeader();
      setError(container, "道爷频倒加载失败。");
    }
  }

  async function manualRefresh() {
    const label = refreshBtn?.querySelector("span");
    if (refreshBtn) refreshBtn.disabled = true;
    if (label) label.textContent = "刷新中...";
    try {
      await refresh();
    } catch (error) {
      console.error("Manual iflow refresh failed:", error);
      alert("道爷频倒模块手动刷新失败。");
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
