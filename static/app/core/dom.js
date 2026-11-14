export function qs(selector, root = document) {
  return root.querySelector(selector);
}

export function qsa(selector, root = document) {
  return Array.from(root.querySelectorAll(selector));
}

export function clearElement(el) {
  if (el) el.innerHTML = "";
}

export function setLoading(el, text) {
  if (el) el.innerHTML = `<div class="loading-state">${text}</div>`;
}

export function setEmpty(el, text) {
  if (el) el.innerHTML = `<div class="empty-state">${text}</div>`;
}

export function setError(el, text) {
  if (el) el.innerHTML = `<div class="error-state">${text}</div>`;
}
