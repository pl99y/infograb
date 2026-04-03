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

function providerLabel(provider) {
  if (!provider) return "未知来源";
  if (provider.startsWith("libretranslate:")) return "LibreTranslate";

  switch (provider) {
    case "failed":
      return "翻译失败";
    case "cached":
      return "缓存";
    case "unknown":
      return "未知来源";
    default:
      return "未知来源";
  }
}

async function translateTitle(ctx, text) {
  const data = await ctx.api.post("/api/translate", {
    texts: [text],
    target_lang: "zh",
  });

  if (!data || !Array.isArray(data.translations) || data.translations.length === 0) {
    throw new Error("No translation returned");
  }

  return {
    text: data.translations[0] || "",
    provider: Array.isArray(data.providers) && data.providers[0] ? data.providers[0] : "unknown",
  };
}

function getCacheKey(article) {
  const stableId = article?.id || article?.url || article?.post_url || article?.title || "untitled";
  return `f1-news-title:${stableId}`;
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

      <div class="f1-news-translation" data-f1-news-translation hidden></div>
      <div class="f1-news-translate-error" data-f1-news-translate-error hidden></div>

      <div class="f1-news-footer">
        <button
          class="f1-news-translate-btn"
          type="button"
          data-f1-news-translate-btn="${index}"
        >翻译</button>
      </div>
    </div>
  `;
}

function setTranslationVisible(btn, translationEl, text, provider) {
  translationEl.innerHTML = `
    <span class="f1-news-translation-label">中文翻译 · ${esc(providerLabel(provider))}</span>
    <div>${esc(text)}</div>
  `;
  translationEl.hidden = false;
  btn.dataset.visible = "1";
  btn.textContent = "收起译文";
}

function setTranslationHidden(btn, translationEl, errorEl) {
  translationEl.hidden = true;
  if (errorEl) errorEl.hidden = true;
  btn.dataset.visible = "0";
  btn.textContent = "翻译";
}

function bindTranslateButtons(ctx, targetEl, articles) {
  const buttons = targetEl.querySelectorAll("[data-f1-news-translate-btn]");

  buttons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const index = Number(btn.getAttribute("data-f1-news-translate-btn"));
      const article = articles[index];
      const itemEl = btn.closest(".f1-news-item");
      const translationEl = itemEl?.querySelector("[data-f1-news-translation]");
      const errorEl = itemEl?.querySelector("[data-f1-news-translate-error]");
      const rawTitle = String(article?.title || "").trim();

      if (!article || !itemEl || !translationEl || !errorEl || !rawTitle) {
        return;
      }

      if (btn.dataset.visible === "1") {
        setTranslationHidden(btn, translationEl, errorEl);
        return;
      }

      const cacheKey = getCacheKey(article);
      const cached = ctx.state.translationCache.get(cacheKey);
      if (cached?.text?.trim()) {
        errorEl.hidden = true;
        setTranslationVisible(btn, translationEl, cached.text, cached.provider || "cached");
        return;
      }

      btn.disabled = true;
      btn.textContent = "翻译中...";
      errorEl.hidden = true;
      errorEl.textContent = "";

      try {
        const result = await translateTitle(ctx, rawTitle);
        ctx.state.translationCache.set(cacheKey, result);
        setTranslationVisible(btn, translationEl, result.text, result.provider);
      } catch (error) {
        console.error("F1 title translation failed:", error);
        errorEl.textContent = "翻译失败，请重试";
        errorEl.hidden = false;
        btn.dataset.visible = "0";
        btn.textContent = "翻译失败，重试";
      } finally {
        btn.disabled = false;
        if (btn.dataset.visible !== "1" && btn.textContent !== "翻译失败，重试") {
          btn.textContent = "翻译";
        }
      }
    });
  });
}

export function renderF1News(ctx, targetEl, articles) {
  if (!targetEl) return;

  if (!Array.isArray(articles) || articles.length === 0) {
    targetEl.innerHTML = `<div class="f1-empty">No F1 news is available right now.</div>`;
    return;
  }

  targetEl.innerHTML = articles.map((article, index) => buildItem(article, index)).join("");
  bindTranslateButtons(ctx, targetEl, articles);
}
