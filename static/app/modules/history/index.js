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
  let focus = null;
  let active = "history";
  let paused = false;
  let animating = false;
  let track = null;
  let timer = null;

  function entries() {
    const result = [];
    const dated = history?.days?.[localDate()] || history;
    const cards = dated?.date === localDate() && Array.isArray(dated.cards)
      ? dated.cards.filter(card => card && typeof card.fact === "string") : [];
    if (cards.length) {
      const index = Math.min(cards.length - 1, Math.max(0, Number(dated.selected_index) || 0));
      const card = cards[index];
      const title = (card.title || card.fact).replace(/[。！？.!?]+$/u, "");
      result.push({kind: "history", text: `历史上的今天 · ${card.year}年　${title}。${card.comment || ""}`,
        title: `${card.fact}\n来源：Wikipedia · CC BY-SA 4.0`});
    }
    if (typeof weather?.text === "string" && weather.text && Date.parse(weather.valid_until) > Date.now()) {
      result.push({kind: "weather", text: `天气占卜 · ${weather.text}`,
        title: "天气数据：Open-Meteo · CC BY 4.0；AI 或规则改写，仅供参考。"});
    }
    if (focus?.text && Date.parse(focus.valid_until) > Date.now()) {
      result.push({kind: "focus", text: `当前关注 · ${focus.text}`,
        href: /^https?:\/\//i.test(focus.url || "") ? focus.url : "",
        title: `${focus.source_name || "来源"} · ${focus.source_time || ""}`});
      if (focus.alert_level === "critical") return result.filter(row => row.kind === "focus");
    }
    return result;
  }

  function fillRow(target, row) {
    const message = document.createElement(row?.href ? "a" : "span");
    if (row?.href) { message.href = row.href; message.target = "_blank"; message.rel = "noopener noreferrer"; }
    message.className = "topline-message";
    message.textContent = row?.text || "历史上的今天 · 今日内容待更新";
    target.replaceChildren(message);
    if (row?.kind === "weather") {
      const credit = document.createElement("a");
      credit.className = "topline-credit";
      credit.textContent = "Open-Meteo";
      credit.href = "https://open-meteo.com/en/licence";
      credit.target = "_blank";
      credit.rel = "noopener noreferrer";
      credit.title = "天气数据来源及 CC BY 4.0 许可；文案已改写";
      target.append(credit);
    }
  }

  function render() {
    if (!container || !line || animating) return;
    const rows = entries();
    const row = rows.find(item => item.kind === active) || rows[0];
    active = row?.kind || "history";
    fillRow(line, row);
    container.title = row?.title || "";
    container.dataset.topline = active;
    container.hidden = false;
    const level = focus?.text && Date.parse(focus.valid_until) > Date.now() ? focus.alert_level : "normal";
    document.querySelector(".top-nav")?.setAttribute("data-alert", ["major", "critical"].includes(level) ? level : "normal");
  }

  function schedule(delay = 5000) {
    clearTimeout(timer);
    timer = setTimeout(turn, delay);
  }

  async function turn() {
    if (!container || !line || !track) return;
    if (paused || document.hidden) { schedule(); return; }
    render(); // Also switches a preloaded history day at midnight, without another request.
    const rows = entries();
    if (rows.length < 2) { schedule(); return; }
    const index = rows.findIndex(row => row.kind === active);
    const next = rows[(index + 1) % rows.length];
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (reduced) {
      active = next.kind;
      render();
      schedule(10000);
      return;
    }
    const incoming = document.createElement("span");
    incoming.className = "topline-row topline-incoming";
    incoming.setAttribute("aria-hidden", "true");
    fillRow(incoming, next);
    incoming.querySelectorAll("a").forEach(link => { link.tabIndex = -1; });
    track.append(incoming);
    animating = true;
    container.dataset.transition = "scrolling";
    const animation = track.animate([{transform: "translateY(0)"}, {transform: "translateY(-100%)"}],
      {duration: 5000, easing: "ease-in-out", fill: "forwards"});
    try {
      await animation.finished;
      active = next.kind;
    } finally {
      incoming.remove();
      animation.cancel();
      animating = false;
      container.dataset.transition = "idle";
      render();
      schedule();
    }
  }

  return {
    init() {
      container?.addEventListener("mouseenter", () => { paused = true; });
      container?.addEventListener("mouseleave", () => { paused = false; });
      container?.addEventListener("focusin", () => { paused = true; });
      container?.addEventListener("focusout", () => { paused = false; });
      if (!container || !line) return;
      const viewport = document.createElement("div");
      viewport.className = "topline-viewport";
      track = document.createElement("div");
      track.className = "topline-track";
      line.classList.add("topline-row");
      container.append(viewport);
      viewport.append(track);
      track.append(line);
      container.dataset.transition = "idle";
      schedule();
    },
    async refresh() {
      const results = await Promise.allSettled([
        ctx.api.get("/api/history/today"), ctx.api.get("/api/local-weather"), ctx.api.get("/api/focus/latest"),
      ]);
      if (results[0].status === "fulfilled") history = results[0].value;
      if (results[1].status === "fulfilled") weather = results[1].value;
      if (results[2].status === "fulfilled") focus = results[2].value;
      render();
    },
  };
}
