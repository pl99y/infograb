import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js";
import { createTranslateButton } from "./translate.js";

function createMediaGrid(ctx, mediaList) {
  const grid = document.createElement("div");
  grid.className = "feed-media-grid";

  mediaList.forEach((media) => {
    if (!media?.media_url) return;
    if (media.media_type === "video") return;

    const item = document.createElement("div");
    item.className = "feed-media-item";

    const img = document.createElement("img");
    img.src = media.media_url;
    img.alt = "Feed media";
    img.loading = "lazy";

    item.appendChild(img);
    item.addEventListener("click", () => ctx.lightbox.open(media.media_url));
    grid.appendChild(item);
  });

  return grid.childElementCount > 0 ? grid : null;
}

export function renderTelegram(ctx, container, items) {
  if (!container) return;
  if (!Array.isArray(items) || items.length === 0) {
    container.innerHTML = `<div class="empty-state">暂无情报内容。</div>`;
    return;
  }

  container.innerHTML = "";

  items.forEach((item) => {
    const wrapper = document.createElement("article");
    wrapper.className = "feed-item";

    const timeValue = item.published_at || item.fetched_at || "";
    const displayText = String(item.text_zh || item.text || "").trim();

    wrapper.innerHTML = `
      <div class="feed-category-line"></div>
      <div class="feed-meta">
        <a class="feed-source" href="${item.post_url || "#"}" target="_blank" rel="noopener noreferrer">${item.source_name || "Unknown Source"}</a>
        <div class="feed-type" data-time-value="${timeValue}"></div>
      </div>
      ${displayText ? `<div class="feed-text"></div>` : ""}
    `;

    const timeEl = wrapper.querySelector(".feed-type");
    if (timeEl) {
      timeEl.textContent = formatRelativeLocalTime(timeValue);
      timeEl.title = formatAbsoluteLocalDateTime(timeValue);
    }

    const textEl = wrapper.querySelector(".feed-text");
    if (textEl) {
      textEl.textContent = displayText;
    }

    const mediaGrid = createMediaGrid(ctx, Array.isArray(item.media) ? item.media : []);
    if (mediaGrid) {
      wrapper.appendChild(mediaGrid);
    }

    const actionHost = document.createElement("div");
    wrapper.appendChild(actionHost);

    if (displayText) {
      createTranslateButton(ctx, item, actionHost);
    }

    container.appendChild(wrapper);
  });
}
