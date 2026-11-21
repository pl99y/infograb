const SHANGHAI_DATE = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
});

function localDate() {
  const fields = Object.fromEntries(SHANGHAI_DATE.formatToParts(new Date()).map(({ type, value }) => [type, value]));
  return `${fields.year}-${fields.month}-${fields.day}`;
}

export function createHistoryModule(ctx) {
  const container = document.getElementById("historyToday");
  const line = document.getElementById("historyTodayText");
  let payload = null;

  function render() {
    if (!container || !line) return;
    container.hidden = false;
    const cards = payload?.date === localDate() && Array.isArray(payload.cards)
      ? payload.cards.filter(card => card && typeof card.fact === "string") : [];
    if (!cards.length) {
      line.textContent = "历史上的今天 · 今日内容待更新";
      container.removeAttribute("title");
      return;
    }
    const index = Math.min(cards.length - 1, Math.max(0, Number(payload.selected_index) || 0));
    const card = cards[index];
    const title = (card.title || card.fact).replace(/[。！？.!?]+$/u, "");
    line.textContent = `历史上的今天 · ${card.year}年　${title}。${card.comment || ""}`;
    const sources = (Array.isArray(card.sources) ? card.sources : []).map(source => source.title).filter(Boolean);
    container.title = `${card.fact}\n来源：Wikipedia${sources.length ? ` · ${sources.join("、")}` : ""} · CC BY-SA 4.0`;
  }

  return {
    init() {},
    async refresh() {
      try {
        payload = await ctx.api.get("/api/history/today");
        render();
      } catch (error) {
        console.error("Failed to load history:", error);
        render();
      }
    },
  };
}
