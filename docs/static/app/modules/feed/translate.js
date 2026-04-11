function createOriginalBlock(text) {
  const block = document.createElement("div");
  block.className = "feed-translation";
  block.innerHTML = `
    <span class="feed-translation-label">原文</span>
    <div></div>
  `;

  const content = block.querySelector("div");
  if (content) content.textContent = text;
  return block;
}

export function createTranslateButton(ctx, item, actionHost) {
  const rawText = String(item.text || "").trim();
  const translatedText = String(item.text_zh || "").trim();

  if (!rawText || !translatedText || rawText === translatedText) return null;

  const actions = document.createElement("div");
  actions.className = "feed-actions";

  const btn = document.createElement("button");
  btn.className = "feed-translate-btn";
  btn.type = "button";
  btn.textContent = "原文";

  let visible = false;
  let originalBlock = null;

  function showOriginal() {
    if (!originalBlock) {
      originalBlock = createOriginalBlock(rawText);
      actionHost.before(originalBlock);
    } else {
      originalBlock.style.display = "";
      const content = originalBlock.querySelector("div");
      if (content) content.textContent = rawText;
    }
    visible = true;
    btn.textContent = "收起原文";
  }

  function hideOriginal() {
    if (originalBlock) originalBlock.style.display = "none";
    visible = false;
    btn.textContent = "原文";
  }

  btn.addEventListener("click", () => {
    if (visible) {
      hideOriginal();
      return;
    }
    showOriginal();
  });

  actions.appendChild(btn);
  actionHost.appendChild(actions);
  return actions;
}
