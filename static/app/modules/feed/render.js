import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "../../core/time.js?v=46";
import { createTranslateButton } from "./translate.js?v=46";

function webUrl(value) {
  if (typeof value !== "string") return null;
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.protocol === "http:" ? url.href : null;
  } catch {
    return null;
  }
}

function createMediaGrid(ctx, mediaList) {
  const grid = document.createElement("div");
  grid.className = "feed-media-grid";

  mediaList.forEach((media) => {
    const url = webUrl(media?.media_url);
    if (!url || media.media_type === "video") return;

    const item = document.createElement("div");
    item.className = "feed-media-item";

    const img = document.createElement("img");
    img.src = url;
    img.alt = "Feed media";
    img.loading = "lazy";

    item.appendChild(img);
    item.addEventListener("click", () => ctx.lightbox.open(url));
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

    const category = document.createElement("div");
    category.className = "feed-category-line";

    const meta = document.createElement("div");
    meta.className = "feed-meta";
    const postUrl = webUrl(item.post_url);
    const source = document.createElement(postUrl ? "a" : "span");
    source.className = "feed-source";
    source.textContent = item.source_name || "Unknown Source";
    if (postUrl) {
      source.href = postUrl;
      source.target = "_blank";
      source.rel = "noopener noreferrer";
    }

    const time = document.createElement("div");
    time.className = "feed-type";
    time.dataset.timeValue = timeValue;
    time.textContent = formatRelativeLocalTime(timeValue);
    time.title = formatAbsoluteLocalDateTime(timeValue);
    meta.append(source, time);
    wrapper.append(category, meta);

    if (displayText) {
      const text = document.createElement("div");
      text.className = "feed-text";
      text.textContent = displayText;
      wrapper.appendChild(text);
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
