import { formatAbsoluteLocalDateTime } from "../../core/time.js?v=46";

const SHANGHAI_DATE = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
});

function localDate() {
  const fields = Object.fromEntries(SHANGHAI_DATE.formatToParts(new Date()).map(({ type, value }) => [type, value]));
  return `${fields.year}-${fields.month}-${fields.day}`;
}

function sourceLink(source) {
  try {
    const url = new URL(source.url);
    if (url.protocol !== "https:" || !["zh.wikipedia.org", "en.wikipedia.org", "www.debian.org"].includes(url.hostname)) return null;
    const link = document.createElement("a");
    link.href = url.href;
    link.textContent = source.title || "查看来源";
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    return link;
  } catch {
    return null;
  }
}

export function createHistoryModule(ctx) {
  const button = document.getElementById("historyToday");
  const line = document.getElementById("historyTodayText");
  const dialog = document.getElementById("historyDialog");
  const heading = document.getElementById("historyDialogTitle");
  const list = document.getElementById("historyCards");
  let payload = null;

  function render() {
    if (!button || !line || !list || !heading) return;
    const cards = Array.isArray(payload?.cards) ? payload.cards.filter(card => card && typeof card.fact === "string") : [];
    button.hidden = false;
    button.disabled = !cards.length;
    if (!cards.length) {
      line.textContent = "历史上的今天 · 暂无内容";
      return;
    }
    const selectedIndex = Math.min(cards.length - 1, Math.max(0, Number(payload.selected_index) || 0));
    const selected = cards[selectedIndex];
    const dateLabel = payload.date === localDate() ? "历史上的今天" : `${payload.date} 的历史`;
    line.textContent = `${dateLabel} · ${selected.year}年　${selected.title}。${selected.comment || ""}`;
    button.title = `${line.textContent}\n点击查看当天 ${cards.length} 条与来源`;
    heading.textContent = `${payload.date} · 历史上的今天`;
    list.replaceChildren();
    cards.forEach((card, index) => {
      const article = document.createElement("article");
      article.className = "history-card";
      const year = document.createElement("div");
      year.className = "history-year";
      year.textContent = `${card.year}年 · ${index + 1}/${cards.length}`;
      const title = document.createElement("h3");
      title.textContent = card.title;
      const fact = document.createElement("p");
      fact.textContent = card.fact;
      const comment = document.createElement("p");
      comment.className = "history-comment";
      comment.textContent = card.comment || "";
      const sources = document.createElement("div");
      sources.className = "history-sources";
      for (const source of Array.isArray(card.sources) ? card.sources : []) {
        const link = sourceLink(source);
        if (link) sources.append(link);
      }
      article.append(year, title, fact, comment, sources);
      const select = document.createElement("button");
      select.type = "button";
      select.className = "history-select";
      select.textContent = index === selectedIndex ? "正在页顶显示" : "显示在页顶";
      select.disabled = index === selectedIndex;
      select.addEventListener("click", () => {
        payload.selected_index = index;
        render();
      });
      article.append(select);
      list.append(article);
    });
    const updated = document.getElementById("historyGeneratedAt");
    if (updated) updated.textContent = payload.generated_at ? `整理于 ${formatAbsoluteLocalDateTime(payload.generated_at)}` : "";
  }

  return {
    init() {
      button?.addEventListener("click", () => {
        if (!button.disabled) dialog?.showModal();
      });
      document.getElementById("historyClose")?.addEventListener("click", () => dialog?.close());
      dialog?.addEventListener("click", event => {
        const bounds = dialog.getBoundingClientRect();
        if (event.target === dialog && (event.clientX < bounds.left || event.clientX > bounds.right || event.clientY < bounds.top || event.clientY > bounds.bottom)) dialog.close();
      });
    },
    async refresh() {
      try {
        payload = await ctx.api.get("/api/history/today");
        render();
      } catch (error) {
        console.error("Failed to load history:", error);
        if (!payload && line && button) {
          line.textContent = "历史上的今天 · 暂不可用";
          button.hidden = false;
          button.disabled = true;
        }
      }
    },
  };
}
