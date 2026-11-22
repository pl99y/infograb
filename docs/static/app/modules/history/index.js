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
  let history = null;
  let weather = null;
  let active = "history";
  let paused = false;
  let animating = false;

  function entries() {
    const result = [];
    const cards = history?.date === localDate() && Array.isArray(history.cards)
      ? history.cards.filter(card => card && typeof card.fact === "string") : [];
    if (cards.length) {
      const index = Math.min(cards.length - 1, Math.max(0, Number(history.selected_index) || 0));
      const card = cards[index];
      const title = (card.title || card.fact).replace(/[。！？.!?]+$/u, "");
      result.push({kind: "history", text: `历史上的今天 · ${card.year}年　${title}。${card.comment || ""}`,
        title: `${card.fact}\n来源：Wikipedia · CC BY-SA 4.0`});
    }
    if (typeof weather?.text === "string" && weather.text && Date.parse(weather.valid_until) > Date.now()) {
      result.push({kind: "weather", text: `天气占卜 · ${weather.text}`,
        title: "天气数据：Open-Meteo · CC BY 4.0；AI 或规则改写，仅供参考。"});
    }
    return result;
  }

  function render() {
    if (!container || !line) return;
    const rows = entries();
    const row = rows.find(item => item.kind === active) || rows[0];
    active = row?.kind || "history";
    const message = document.createElement("span");
    message.className = "topline-message";
    message.textContent = row?.text || "历史上的今天 · 今日内容待更新";
    line.replaceChildren(message);
    if (active === "weather") {
      const credit = document.createElement("a");
      credit.className = "topline-credit";
      credit.textContent = "Open-Meteo";
      credit.href = "https://open-meteo.com/en/licence";
      credit.target = "_blank";
      credit.rel = "noopener noreferrer";
      credit.title = "天气数据来源及 CC BY 4.0 许可；文案已改写";
      line.append(credit);
    }
    container.title = row?.title || "";
    container.dataset.topline = active;
    container.hidden = false;
  }

  return {
    init() {
      container?.addEventListener("mouseenter", () => { paused = true; });
      container?.addEventListener("mouseleave", () => { paused = false; });
      container?.addEventListener("focusin", () => { paused = true; });
      container?.addEventListener("focusout", () => { paused = false; });
      setInterval(async () => {
        if (!container || !line || paused || animating || document.hidden) return;
        const rows = entries();
        if (rows.length < 2) { render(); return; }
        animating = true;
        try {
          const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
          if (!reduced) await line.animate([{transform: "translateY(0)", opacity: 1},
            {transform: "translateY(-100%)", opacity: 0}], {duration: 180}).finished;
          active = active === "history" ? "weather" : "history";
          render();
          if (!reduced) await line.animate([{transform: "translateY(100%)", opacity: 0},
            {transform: "translateY(0)", opacity: 1}], {duration: 180}).finished;
        } finally { animating = false; }
      }, 10000);
    },
    async refresh() {
      const results = await Promise.allSettled([
        ctx.api.get("/api/history/today"), ctx.api.get("/api/local-weather"),
      ]);
      if (results[0].status === "fulfilled") history = results[0].value;
      if (results[1].status === "fulfilled") weather = results[1].value;
      render();
    },
  };
}
