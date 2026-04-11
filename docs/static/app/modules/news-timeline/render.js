import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";

const SOURCE_LABELS = {
  zaobao: "联合早报",
  mingpao: "明报",
  hk01: "香港01",
};

function buildMeta(item) {
  const parts = [];
  const sourceName = SOURCE_LABELS[item.source_key] || item.source_name || "未知来源";
  parts.push(sourceName);
  if (item.channel) parts.push(item.channel);
  if (item.topic) parts.push(item.topic);
  return parts.join(" · ");
}

export function renderNewsTimeline(ctx, container, items) {
  if (!container) return;
  if (!Array.isArray(items) || items.length === 0) {
    container.innerHTML = `<div class="empty-state">过去 12 小时暂无新闻。</div>`;
    return;
  }

  container.innerHTML = "";

  items.forEach((item) => {
    const article = document.createElement("article");
    article.className = "news-timeline-item";

    const timeValue = item.published_at || item.fetched_at || item.created_at || "";
    const metaText = buildMeta(item);
    const authorText = Array.isArray(item.author_names) && item.author_names.length > 0
      ? `<div class="news-timeline-authors">${item.author_names.join(" / ")}</div>`
      : "";

    article.innerHTML = `
      <div class="news-timeline-marker" aria-hidden="true"></div>
      <div class="news-timeline-content">
        <div class="news-timeline-time" data-news-time-value="${timeValue}"></div>
        <a class="news-timeline-title" href="${item.url || "#"}" target="_blank" rel="noopener noreferrer">${item.title || "未命名新闻"}</a>
        <div class="news-timeline-meta">${metaText}</div>
        ${authorText}
      </div>
    `;

    const timeEl = article.querySelector(".news-timeline-time");
    if (timeEl) {
      timeEl.textContent = formatRelativeLocalTime(timeValue);
      timeEl.title = formatAbsoluteLocalDateTime(timeValue);
    }

    container.appendChild(article);
  });
}
