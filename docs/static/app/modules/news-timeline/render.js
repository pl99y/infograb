import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";

const SOURCE_LABELS = {
  zaobao: "联合早报",
  mingpao: "明报",
  hk01: "香港01",
};

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

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
      ? `<div class="news-timeline-authors">${esc(item.author_names.join(" / "))}</div>`
      : "";

    article.innerHTML = `
      <div class="news-timeline-marker" aria-hidden="true"></div>
      <div class="news-timeline-content">
        <div class="news-timeline-time" data-news-time-value="${esc(timeValue)}"></div>
        <a class="news-timeline-title" href="${esc(item.url || "#")}" target="_blank" rel="noopener noreferrer">${esc(item.title || "未命名新闻")}</a>
        <div class="news-timeline-meta">${esc(metaText)}</div>
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

function renderAiDigest(digest) {
  if (!digest || digest.ok !== true) {
    const reason = digest?.error ? `（${esc(digest.error)}）` : "";
    return `
      <section class="hotsearch-digest hotsearch-digest-muted">
        <div class="hotsearch-digest-kicker">AI 重点摘要</div>
        <div class="hotsearch-digest-summary">AI 摘要暂不可用${reason}，下方热搜榜仍可正常查看。</div>
      </section>
    `;
  }

  const priority = Array.isArray(digest.priority_items) ? digest.priority_items.slice(0, 6) : [];
  const priorityMarkup = priority.length > 0
    ? `
      <div class="hotsearch-priority-list">
        ${priority.map((item) => `
          <div class="hotsearch-priority-item">
            <div class="hotsearch-priority-title">${esc(item.title || "未命名话题")}</div>
            <div class="hotsearch-priority-reason">
              ${item.category ? `<span>${esc(item.category)}</span>` : ""}
              ${item.reason ? `<span>${esc(item.reason)}</span>` : ""}
            </div>
          </div>
        `).join("")}
      </div>
    `
    : "";

  const noMajor = digest.no_major_news === true
    ? `<div class="hotsearch-no-major">当前热搜中未发现明显需要重点关注的公共新闻。</div>`
    : "";

  return `
    <section class="hotsearch-digest">
      <div class="hotsearch-digest-kicker">AI 重点摘要</div>
      <div class="hotsearch-digest-summary">${esc(digest.summary || "暂无摘要。")}</div>
      ${priorityMarkup}
      ${noMajor}
      ${digest.noise_note ? `<div class="hotsearch-noise-note">${esc(digest.noise_note)}</div>` : ""}
    </section>
  `;
}

function renderSourceBlock(source) {
  const items = Array.isArray(source?.items) ? source.items : [];
  const title = source?.source_name || "热搜源";
  const ok = source?.ok !== false;
  const error = source?.error || source?.parse_error || "";

  if (!ok || items.length === 0) {
    return `
      <section class="hotsearch-source">
        <div class="hotsearch-source-head">
          <div class="hotsearch-source-title">${esc(title)}</div>
          <div class="hotsearch-source-count">0</div>
        </div>
        <div class="empty-state compact">暂无可用热搜${error ? `：${esc(error)}` : ""}</div>
      </section>
    `;
  }

  return `
    <section class="hotsearch-source">
      <div class="hotsearch-source-head">
        <div class="hotsearch-source-title">${esc(title)}</div>
        <div class="hotsearch-source-count">${items.length}</div>
      </div>
      <div class="hotsearch-list">
        ${items.map((item) => `
          <a class="hotsearch-item" href="${esc(item.url || "#")}" target="_blank" rel="noopener noreferrer">
            <span class="hotsearch-rank">${esc(item.rank || "—")}</span>
            <span class="hotsearch-title">${esc(item.title || "未命名话题")}</span>
            ${item.metric ? `<span class="hotsearch-metric">${esc(item.metric)}</span>` : ""}
          </a>
        `).join("")}
      </div>
    </section>
  `;
}

export function renderHotsearch(ctx, container, payload) {
  if (!container) return;

  const sources = Array.isArray(payload?.sources) ? payload.sources : [];
  const itemsCount = Number(payload?.merged_count || 0);
  const generatedAt = payload?.generated_at || "";

  if (!payload || sources.length === 0) {
    container.innerHTML = `<div class="empty-state">暂无热搜数据。</div>`;
    return;
  }

  container.innerHTML = `
    <div class="hotsearch-wrap">
      <div class="hotsearch-toolbar">
        <div>
          <div class="hotsearch-title-main">热搜观察</div>
          <div class="hotsearch-subtitle">
            微博 20 · 百度 20 · B站 10
            ${itemsCount ? ` · 合并 ${itemsCount} 条` : ""}
            ${generatedAt ? ` · <span title="${esc(formatAbsoluteLocalDateTime(generatedAt))}">${esc(formatRelativeLocalTime(generatedAt))}更新</span>` : ""}
          </div>
        </div>
        <div class="hotsearch-source-pill">TopHub</div>
      </div>

      ${renderAiDigest(payload.ai_digest)}

      <div class="hotsearch-grid">
        ${sources.map(renderSourceBlock).join("")}
      </div>
    </div>
  `;
}
