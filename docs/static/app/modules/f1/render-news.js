function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatTime(value) {
  if (!value) return "";
  try {
    const dt = new Date(value);
    return dt.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return String(value);
  }
}

function buildMeta(article) {
  const parts = [];
  const source = article?.source_name || "Autosport";
  const time = formatTime(article?.published_at || article?.fetched_at);
  if (source) parts.push(source);
  if (time) parts.push(time);
  return parts.join(" · ");
}


function buildItem(article, index) {
  const title = article?.title || "Untitled";
  const href = article?.post_url || article?.url || "#";
  const meta = buildMeta(article);

  return `
    <div class="f1-news-item" data-f1-news-item="${index}">
      <div class="f1-news-head">
        <a class="f1-news-title-link" href="${esc(href)}" target="_blank" rel="noreferrer">
          <div class="f1-news-title">${esc(title)}</div>
        </a>
        ${meta ? `<div class="f1-news-time">${esc(meta)}</div>` : ""}
      </div>
    </div>
  `;
}

export function renderF1News(ctx, targetEl, articles) {
  if (!targetEl) return;

  if (!Array.isArray(articles) || articles.length === 0) {
    targetEl.innerHTML = `<div class="f1-empty">No F1 news is available right now.</div>`;
    return;
  }

  targetEl.innerHTML = articles.map((article, index) => buildItem(article, index)).join("");
}
