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

function createTranslationBlock(text, provider) {
  const block = document.createElement("div");
  block.className = "feed-translation";
  block.innerHTML = `
    <span class="feed-translation-label">中文翻译 · ${providerLabel(provider)}</span>
    <div>${text}</div>
  `;
  return block;
}

async function translateOne(ctx, text) {
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

export function createTranslateButton(ctx, item, actionHost) {
  const rawText = String(item.text || "").trim();
  if (!rawText) return null;

  const actions = document.createElement("div");
  actions.className = "feed-actions";

  const btn = document.createElement("button");
  btn.className = "feed-translate-btn";
  btn.type = "button";
  btn.textContent = "翻译";

  let visible = false;
  let translationBlock = null;

  function showTranslation(text, provider) {
    if (!translationBlock) {
      translationBlock = createTranslationBlock(text, provider);
      actionHost.before(translationBlock);
    } else {
      const label = translationBlock.querySelector(".feed-translation-label");
      const content = translationBlock.querySelector("div");
      if (label) label.textContent = `中文翻译 · ${providerLabel(provider)}`;
      if (content) content.textContent = text;
      translationBlock.style.display = "";
    }
    visible = true;
    btn.textContent = "收起译文";
  }

  function hideTranslation() {
    if (translationBlock) translationBlock.style.display = "none";
    visible = false;
    btn.textContent = "翻译";
  }

  btn.addEventListener("click", async () => {
    if (visible) {
      hideTranslation();
      return;
    }

    const cached = ctx.state.translationCache.get(item.id);
    if (cached?.text?.trim()) {
      showTranslation(cached.text, cached.provider || "cached");
      return;
    }

    if (item.text_zh && item.text_zh.trim()) {
      showTranslation(item.text_zh, "unknown");
      return;
    }

    btn.disabled = true;
    btn.textContent = "翻译中...";

    try {
      const result = await translateOne(ctx, rawText);
      ctx.state.translationCache.set(item.id, result);
      showTranslation(result.text, result.provider);
    } catch (error) {
      console.error("Translation failed:", error);
      btn.textContent = "翻译失败，重试";
      return;
    } finally {
      btn.disabled = false;
      if (!visible && btn.textContent !== "翻译失败，重试") {
        btn.textContent = "翻译";
      }
    }
  });

  actions.appendChild(btn);
  actionHost.appendChild(actions);
  return actions;
}
