import { MOBILE_BREAKPOINT, STORAGE_KEYS } from "../../core/constants.js";

const TOTAL_COLUMNS = 24;
const BOARD_GAP_X = 8;
const BOARD_GAP_Y = 8;
const Y_STEP = 12;
const WIDTH_STEP = 1;
const HEIGHT_STEP = 12;

const DEFAULT_MIN_W = 4;
const DEFAULT_MIN_H = 320;
const DEFAULT_MAX_H = 1600;

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function roundToStep(value, step) {
  return Math.round(value / step) * step;
}

function getPanels(container) {
  return Array.from(container.querySelectorAll(".grid-stack-item[gs-id]"));
}

function getPanelId(panel) {
  return panel.getAttribute("gs-id");
}

function getPanelTitle(panel) {
  return (
    panel.dataset.panelTitle ||
    panel.querySelector(".section-header h3")?.textContent?.trim() ||
    getPanelId(panel)
  );
}

function getPanelDefaults(panel) {
  return {
    x: Number(panel.dataset.defaultX) || 0,
    y: Number(panel.dataset.defaultY) || 0,
    w: Number(panel.dataset.defaultW) || 8,
    h: Number(panel.dataset.defaultH) || 420,
    minW: Number(panel.dataset.minW) || 4,
    minH: Number(panel.dataset.minH) || DEFAULT_MIN_H,
    maxH: Number(panel.dataset.maxH) || DEFAULT_MAX_H,
  };
}

function buildDefaultPanelsState(panels) {
  const result = {};
  let cursorX = 0;
  let cursorY = 0;
  let rowMaxH = 0;

  for (const panel of panels) {
    const id = getPanelId(panel);
    const d = getPanelDefaults(panel);

    if (cursorX + d.w > TOTAL_COLUMNS) {
      cursorX = 0;
      cursorY += rowMaxH + BOARD_GAP_Y;
      rowMaxH = 0;
    }

    result[id] = {
      x: cursorX,
      y: cursorY,
      w: d.w,
      h: d.h,
    };

    cursorX += d.w;
    rowMaxH = Math.max(rowMaxH, d.h);
  }

  return result;
}

function normalizeOnePanelState(panel, raw, fallback) {
  const d = getPanelDefaults(panel);

  const w = clamp(
    roundToStep(Number(raw?.w ?? fallback?.w ?? d.w), WIDTH_STEP),
    d.minW,
    TOTAL_COLUMNS
  );

  const h = clamp(
    roundToStep(Number(raw?.h ?? fallback?.h ?? d.h), HEIGHT_STEP),
    d.minH,
    d.maxH
  );

  const x = clamp(
    roundToStep(Number(raw?.x ?? fallback?.x ?? d.x), WIDTH_STEP),
    0,
    TOTAL_COLUMNS - w
  );

  const y = Math.max(
    0,
    roundToStep(Number(raw?.y ?? fallback?.y ?? d.y), Y_STEP)
  );

  return { x, y, w, h };
}

function normalizeState(container, rawState) {
  const panels = getPanels(container);
  const ids = panels.map(getPanelId);
  const defaults = buildDefaultPanelsState(panels);

  const rawOrder = Array.isArray(rawState?.order) ? rawState.order : [];
  const order = rawOrder.filter((id) => ids.includes(id));
  ids.forEach((id) => {
    if (!order.includes(id)) {
      order.push(id);
    }
  });

  let hidden = Array.isArray(rawState?.hidden)
    ? rawState.hidden.filter((id) => ids.includes(id))
    : [];

  if (hidden.length >= ids.length) {
    hidden = hidden.slice(0, ids.length - 1);
  }

  const panelsState = {};
  for (const panel of panels) {
    const id = getPanelId(panel);
    panelsState[id] = normalizeOnePanelState(
      panel,
      rawState?.panels?.[id],
      defaults[id]
    );
  }

  return {
    order,
    hidden,
    panels: panelsState,
  };
}

function horizontalRangesOverlap(a, b) {
  return !(a.x + a.w <= b.x || b.x + b.w <= a.x);
}

function rectsOverlap(a, b) {
  return !(
    a.x + a.w <= b.x ||
    b.x + b.w <= a.x ||
    a.y + a.h <= b.y ||
    b.y + b.h <= a.y
  );
}

function needsVerticalSeparation(a, b) {
  if (!horizontalRangesOverlap(a, b)) return false;

  const aBottom = a.y + a.h;
  const bBottom = b.y + b.h;

  // 真重叠
  if (!(aBottom <= b.y || bBottom <= a.y)) return true;

  // 没重叠，但上下距离小于最小间隙
  const gapAB = b.y - aBottom;
  const gapBA = a.y - bBottom;

  if (gapAB >= 0 && gapAB < BOARD_GAP_Y) return true;
  if (gapBA >= 0 && gapBA < BOARD_GAP_Y) return true;

  return false;
}

export function createLayoutModule(ctx) {
  const dashboardGrid = document.getElementById("dashboardGrid");
  const layoutEditBtn = document.getElementById("layoutEditBtn");
  const layoutEditBanner = document.getElementById("layoutEditBanner");
  const layoutResetBtn = document.getElementById("layoutResetBtn");
  const layoutDoneBtn = document.getElementById("layoutDoneBtn");
  const layoutPanelsBtn = document.getElementById("layoutPanelsBtn");
  const layoutPanelPicker = document.getElementById("layoutPanelPicker");
  const layoutPanelList = document.getElementById("layoutPanelList");
  const layoutPanelPickerCloseBtn = document.getElementById("layoutPanelPickerCloseBtn");

  let state = normalizeState(
    dashboardGrid,
    ctx.store.getJSON(STORAGE_KEYS.layout, {})
  );

  let pickerOpen = false;

  function isMobile() {
    return window.innerWidth <= MOBILE_BREAKPOINT;
  }

  function getVisibleIds() {
    return state.order.filter((id) => !state.hidden.includes(id));
  }

  function ensureAtLeastOneVisible(nextHidden) {
    const allIds = getPanels(dashboardGrid).map(getPanelId);
    const visibleCount = allIds.filter((id) => !nextHidden.includes(id)).length;
    return visibleCount > 0;
  }

  function saveState() {
    ctx.store.setJSON(STORAGE_KEYS.layout, state);
    document.dispatchEvent(
      new CustomEvent("ig:layout-visibility-changed", {
        detail: { hidden: [...state.hidden] },
      })
    );
  }

  function applyVisibility() {
    const visibleIds = getVisibleIds();

    getPanels(dashboardGrid).forEach((panel) => {
      const id = getPanelId(panel);
      panel.hidden = state.hidden.includes(id);
    });

    const mobileButtons = Array.from(document.querySelectorAll(".mobile-tab-btn[data-panel]"));
    mobileButtons.forEach((btn) => {
      const id = btn.dataset.panel;
      btn.hidden = !visibleIds.includes(id);
    });

    if (!visibleIds.includes(ctx.state.mobilePanel)) {
      ctx.state.mobilePanel = visibleIds[0] || "";
      if (ctx.state.mobilePanel) {
        ctx.store.set(STORAGE_KEYS.mobilePanel, ctx.state.mobilePanel);
      }
    }

    getPanels(dashboardGrid).forEach((panel) => {
      const id = getPanelId(panel);
      panel.classList.toggle(
        "mobile-active",
        !panel.hidden && id === ctx.state.mobilePanel
      );
    });

    document.dispatchEvent(
      new CustomEvent("ig:layout-visibility-changed", {
        detail: { hidden: [...state.hidden] },
      })
    );
  }

  function bringToFront(panel) {
    getPanels(dashboardGrid).forEach((p) => {
      p.style.zIndex = p === panel ? "50" : "1";
    });
  }

  function resetZIndex() {
    getPanels(dashboardGrid).forEach((p) => {
      p.style.zIndex = "";
    });
  }

  function relaxVerticalSpacing(anchorId) {
    const visibleIds = getVisibleIds();
    const queue = [anchorId];
    const visited = new Set();

    while (queue.length) {
      const currentId = queue.shift();
      if (visited.has(currentId)) continue;
      visited.add(currentId);

      const current = state.panels[currentId];
      if (!current) continue;

      for (const otherId of visibleIds) {
        if (otherId === currentId) continue;

        const other = state.panels[otherId];
        if (!other) continue;

        if (!needsVerticalSeparation(current, other)) continue;

        // 只把“更下面”的那个往下轻推
        if (other.y >= current.y) {
          other.y = roundToStep(current.y + current.h + BOARD_GAP_Y, Y_STEP);
          queue.push(otherId);
        } else {
          current.y = roundToStep(other.y + other.h + BOARD_GAP_Y, Y_STEP);
          queue.push(currentId);
        }
      }
    }
  }

  function buildDisplayBands(visibleIds) {
    const rowStarts = [...new Set(
      visibleIds
        .map((id) => state.panels[id]?.y)
        .filter((y) => Number.isFinite(y))
    )].sort((a, b) => a - b);

    if (!rowStarts.length) {
      return { rowStarts: [], rowTops: [], rowHeights: [] };
    }

    const rowHeights = rowStarts.map(() => 0);

    const getSpan = (box, rowIndex) => {
      const bottom = box.y + box.h;
      let span = 1;
      for (let i = rowIndex + 1; i < rowStarts.length; i += 1) {
        if (rowStarts[i] < bottom - 1) span += 1;
        else break;
      }
      return span;
    };

    for (let rowIndex = 0; rowIndex < rowStarts.length; rowIndex += 1) {
      const rowStart = rowStarts[rowIndex];
      const singles = visibleIds
        .map((id) => state.panels[id])
        .filter((box) => box && box.y === rowStart && getSpan(box, rowIndex) === 1);

      if (singles.length) {
        rowHeights[rowIndex] = Math.max(...singles.map((box) => box.h));
      }
    }

    for (let rowIndex = 0; rowIndex < rowStarts.length; rowIndex += 1) {
      if (rowHeights[rowIndex] > 0) continue;

      const starters = visibleIds
        .map((id) => state.panels[id])
        .filter((box) => box && box.y === rowStarts[rowIndex]);

      if (starters.length) {
        rowHeights[rowIndex] = Math.max(
          ...starters.map((box) => {
            const span = getSpan(box, rowIndex);
            const gapCost = BOARD_GAP_Y * (span - 1);
            return Math.max(DEFAULT_MIN_H, Math.ceil((box.h - gapCost) / span));
          })
        );
      } else {
        rowHeights[rowIndex] = DEFAULT_MIN_H;
      }
    }

    for (let rowIndex = 0; rowIndex < rowStarts.length; rowIndex += 1) {
      const starters = visibleIds
        .map((id) => state.panels[id])
        .filter((box) => box && box.y === rowStarts[rowIndex]);

      for (const box of starters) {
        const span = getSpan(box, rowIndex);
        const current = rowHeights
          .slice(rowIndex, rowIndex + span)
          .reduce((sum, h) => sum + h, 0) + BOARD_GAP_Y * (span - 1);

        if (current < box.h) {
          rowHeights[rowIndex + span - 1] += box.h - current;
        }
      }
    }

    const rowTops = [];
    let cursorTop = 0;
    for (let i = 0; i < rowStarts.length; i += 1) {
      rowTops.push(cursorTop);
      cursorTop += rowHeights[i] + BOARD_GAP_Y;
    }

    return { rowStarts, rowTops, rowHeights };
  }

  function applyDesktopRawLayout() {
    const panels = getPanels(dashboardGrid);
    const visibleIds = getVisibleIds();
    const gridWidth = dashboardGrid.clientWidth;

    if (!gridWidth) return;

    const totalGap = BOARD_GAP_X * (TOTAL_COLUMNS - 1);
    const colWidth = Math.max(1, (gridWidth - totalGap) / TOTAL_COLUMNS);
    let maxBottom = 0;

    for (const id of visibleIds) {
      const panel = panels.find((p) => getPanelId(p) === id);
      const box = state.panels[id];
      if (!panel || !box) continue;

      const left = box.x * (colWidth + BOARD_GAP_X);
      const top = box.y;
      const width = box.w * colWidth + (box.w - 1) * BOARD_GAP_X;
      const height = box.h;

      panel.style.position = "absolute";
      panel.style.left = `${left}px`;
      panel.style.top = `${top}px`;
      panel.style.width = `${width}px`;
      panel.style.height = `${height}px`;

      maxBottom = Math.max(maxBottom, top + height);
    }

    dashboardGrid.style.height = `${Math.max(320, maxBottom)}px`;
  }

  function applyDesktopAlignedLayout() {
    const panels = getPanels(dashboardGrid);
    const visibleIds = getVisibleIds();
    const gridWidth = dashboardGrid.clientWidth;

    if (!gridWidth) return;

    const totalGap = BOARD_GAP_X * (TOTAL_COLUMNS - 1);
    const colWidth = Math.max(1, (gridWidth - totalGap) / TOTAL_COLUMNS);
    const displayBands = buildDisplayBands(visibleIds);

    const getSpan = (box, rowIndex) => {
      const bottom = box.y + box.h;
      let span = 1;
      for (let i = rowIndex + 1; i < displayBands.rowStarts.length; i += 1) {
        if (displayBands.rowStarts[i] < bottom - 1) span += 1;
        else break;
      }
      return span;
    };

    let maxBottom = 0;

    for (const id of visibleIds) {
      const panel = panels.find((p) => getPanelId(p) === id);
      const box = state.panels[id];
      if (!panel || !box) continue;

      const left = box.x * (colWidth + BOARD_GAP_X);
      const width = box.w * colWidth + (box.w - 1) * BOARD_GAP_X;
      const rowIndex = displayBands.rowStarts.findIndex((y) => y === box.y);
      const top = rowIndex >= 0 ? displayBands.rowTops[rowIndex] : box.y;
      const span = rowIndex >= 0 ? getSpan(box, rowIndex) : 1;
      const height = rowIndex >= 0
        ? displayBands.rowHeights
            .slice(rowIndex, rowIndex + span)
            .reduce((sum, h) => sum + h, 0) + BOARD_GAP_Y * (span - 1)
        : box.h;

      panel.style.position = "absolute";
      panel.style.left = `${left}px`;
      panel.style.top = `${top}px`;
      panel.style.width = `${width}px`;
      panel.style.height = `${height}px`;

      maxBottom = Math.max(maxBottom, top + height);
    }

    dashboardGrid.style.height = `${Math.max(320, maxBottom)}px`;
  }

  function applyDesktopLayout() {
    if (ctx.state.layoutEditMode) {
      applyDesktopRawLayout();
    } else {
      applyDesktopAlignedLayout();
    }
  }

  function clearDesktopStylesForMobile() {
    dashboardGrid.style.height = "";
    getPanels(dashboardGrid).forEach((panel) => {
      panel.style.position = "";
      panel.style.left = "";
      panel.style.top = "";
      panel.style.width = "";
      panel.style.height = "";
      panel.style.zIndex = "";
    });
  }

  function applyOrder() {
    const map = new Map(getPanels(dashboardGrid).map((panel) => [getPanelId(panel), panel]));
    state.order.forEach((id) => {
      const panel = map.get(id);
      if (panel) {
        dashboardGrid.appendChild(panel);
      }
    });
  }

  function renderPanelPicker() {
    if (!layoutPanelList) return;

    const visibleIds = getVisibleIds();

    layoutPanelList.innerHTML = state.order
      .map((id) => getPanels(dashboardGrid).find((panel) => getPanelId(panel) === id))
      .filter(Boolean)
      .map((panel) => {
        const id = getPanelId(panel);
        const title = getPanelTitle(panel);
        const checked = visibleIds.includes(id);

        return `
          <label class="layout-panel-option">
            <input type="checkbox" data-panel-check="${id}" ${checked ? "checked" : ""}>
            <div class="layout-panel-option-text">
              <div class="layout-panel-option-title">${title}</div>
              <div class="layout-panel-option-sub">${id}</div>
            </div>
          </label>
        `;
      })
      .join("");

    layoutPanelList.querySelectorAll("input[data-panel-check]").forEach((input) => {
      input.addEventListener("change", () => {
        const id = input.dataset.panelCheck;
        const nextHidden = [...state.hidden];

        if (input.checked) {
          state.hidden = nextHidden.filter((item) => item !== id);
        } else {
          if (!nextHidden.includes(id)) {
            nextHidden.push(id);
          }

          if (!ensureAtLeastOneVisible(nextHidden)) {
            input.checked = true;
            return;
          }

          state.hidden = nextHidden;
        }

        applyLayout();
        saveState();
      });
    });
  }

  function applyLayout() {
    state = normalizeState(dashboardGrid, state);
    applyOrder();
    applyVisibility();

    if (isMobile()) {
      clearDesktopStylesForMobile();
      if (ctx.state.layoutEditMode) {
        setEditMode(false);
      }
      renderPanelPicker();
      return;
    }

    if (ctx.state.layoutEditMode) {
      applyDesktopRawLayout();
    } else {
      applyDesktopAlignedLayout();
    }
    renderPanelPicker();
  }

  function updateEditUI() {
    document.body.classList.toggle("edit-layout-mode", !!ctx.state.layoutEditMode);

    const icon = layoutEditBtn?.querySelector("i");
    const text = layoutEditBtn?.querySelector("span");

    if (icon) {
      icon.className = ctx.state.layoutEditMode ? "ph ph-check" : "ph ph-squares-four";
    }

    if (text) {
      text.textContent = ctx.state.layoutEditMode ? "编辑中" : "编辑布局";
    }

    if (layoutEditBanner) {
      layoutEditBanner.style.display = ctx.state.layoutEditMode ? "flex" : "";
    }

    if (!ctx.state.layoutEditMode) {
      closePanelPicker();
    }

    getPanels(dashboardGrid).forEach((panel) => {
      const header = panel.querySelector(".section-header");
      if (header) {
        header.dataset.dragReady = ctx.state.layoutEditMode && !isMobile() && !panel.hidden ? "1" : "0";
      }
    });
  }

  function setEditMode(enabled) {
    ctx.state.layoutEditMode = Boolean(enabled) && !isMobile();
    updateEditUI();
    applyLayout();
  }

  function openPanelPicker() {
    if (!ctx.state.layoutEditMode || isMobile()) return;
    pickerOpen = true;
    layoutPanelPicker?.classList.add("open");
  }

  function closePanelPicker() {
    pickerOpen = false;
    layoutPanelPicker?.classList.remove("open");
  }

  function togglePanelPicker() {
    if (pickerOpen) closePanelPicker();
    else openPanelPicker();
  }

  function bindHeaderDrag(panel) {
    if (panel.dataset.xyDragBound === "1") return;
    panel.dataset.xyDragBound = "1";

    const header = panel.querySelector(".section-header");
    if (!header) return;

    header.addEventListener("pointerdown", (event) => {
      if (!ctx.state.layoutEditMode || isMobile() || panel.hidden) return;
      if (header.dataset.dragReady !== "1") return;
      if (event.target.closest("button, a, input, label")) return;

      event.preventDefault();

      const id = getPanelId(panel);
      const start = state.panels[id];
      if (!start) return;

      bringToFront(panel);
      panel.classList.add("dragging");
      document.body.classList.add("layout-dragging");

      const gridRect = dashboardGrid.getBoundingClientRect();
      const totalGap = BOARD_GAP_X * (TOTAL_COLUMNS - 1);
      const colWidth = Math.max(1, (gridRect.width - totalGap) / TOTAL_COLUMNS);
      const colStepPx = colWidth + BOARD_GAP_X;

      const startClientX = event.clientX;
      const startClientY = event.clientY;
      const startX = start.x;
      const startY = start.y;

      const onMove = (moveEvent) => {
        const dx = moveEvent.clientX - startClientX;
        const dy = moveEvent.clientY - startClientY;

        state.panels[id].x = clamp(
          roundToStep(startX + dx / colStepPx, WIDTH_STEP),
          0,
          TOTAL_COLUMNS - state.panels[id].w
        );
        state.panels[id].y = Math.max(
          0,
          roundToStep(startY + dy, Y_STEP)
        );

        applyDesktopLayout();
      };

      const onUp = () => {
        panel.classList.remove("dragging");
        document.body.classList.remove("layout-dragging");
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);

        relaxVerticalSpacing(id);
        applyLayout();
        saveState();
        resetZIndex();
      };

      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    });
  }

  function bindHorizontalResize(panel, handle) {
    if (handle.dataset.boundX === "1") return;
    handle.dataset.boundX = "1";

    handle.addEventListener("pointerdown", (event) => {
      if (!ctx.state.layoutEditMode || isMobile() || panel.hidden) return;

      event.preventDefault();
      event.stopPropagation();

      const id = getPanelId(panel);
      const start = state.panels[id];
      const defaults = getPanelDefaults(panel);
      if (!start) return;

      bringToFront(panel);
      handle.classList.add("active");
      document.body.classList.add("layout-resizing-x");

      const gridRect = dashboardGrid.getBoundingClientRect();
      const totalGap = BOARD_GAP_X * (TOTAL_COLUMNS - 1);
      const colWidth = Math.max(1, (gridRect.width - totalGap) / TOTAL_COLUMNS);
      const colStepPx = colWidth + BOARD_GAP_X;
      const startClientX = event.clientX;
      const startW = start.w;

      const onMove = (moveEvent) => {
        const dx = moveEvent.clientX - startClientX;
        const deltaCols = roundToStep(dx / colStepPx, WIDTH_STEP);

        state.panels[id].w = clamp(
          startW + deltaCols,
          defaults.minW,
          TOTAL_COLUMNS - state.panels[id].x
        );

        applyDesktopLayout();
      };

      const onUp = () => {
        handle.classList.remove("active");
        document.body.classList.remove("layout-resizing-x");
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);

        relaxVerticalSpacing(id);
        applyLayout();
        saveState();
        resetZIndex();
      };

      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    });
  }

  function bindVerticalResize(panel, handle) {
    if (handle.dataset.boundY === "1") return;
    handle.dataset.boundY = "1";

    handle.addEventListener("pointerdown", (event) => {
      if (!ctx.state.layoutEditMode || isMobile() || panel.hidden) return;

      event.preventDefault();
      event.stopPropagation();

      const id = getPanelId(panel);
      const start = state.panels[id];
      const defaults = getPanelDefaults(panel);
      if (!start) return;

      bringToFront(panel);
      handle.classList.add("active");
      document.body.classList.add("layout-resizing-y");

      const startClientY = event.clientY;
      const startH = start.h;

      const onMove = (moveEvent) => {
        const dy = moveEvent.clientY - startClientY;

        state.panels[id].h = clamp(
          roundToStep(startH + dy, HEIGHT_STEP),
          defaults.minH,
          defaults.maxH
        );

        applyDesktopLayout();
      };

      const onUp = () => {
        handle.classList.remove("active");
        document.body.classList.remove("layout-resizing-y");
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);

        relaxVerticalSpacing(id);
        applyLayout();
        saveState();
        resetZIndex();
      };

      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    });
  }

  function ensureResizeHandles(panel) {
    const col = panel.querySelector(".grid-col");
    if (!col) return;

    let xHandle = col.querySelector(".panel-resize-x");
    if (!xHandle) {
      xHandle = document.createElement("div");
      xHandle.className = "panel-resize-x";
      xHandle.dataset.panel = getPanelId(panel);
      col.appendChild(xHandle);
    }

    let yHandle = col.querySelector(".panel-resize-y");
    if (!yHandle) {
      yHandle = document.createElement("div");
      yHandle.className = "panel-resize-y";
      yHandle.dataset.panel = getPanelId(panel);
      col.appendChild(yHandle);
    }

    bindHorizontalResize(panel, xHandle);
    bindVerticalResize(panel, yHandle);
  }

  function installPerPanelFeatures() {
    getPanels(dashboardGrid).forEach((panel) => {
      bindHeaderDrag(panel);
      ensureResizeHandles(panel);
    });
  }

  function resetLayout() {
    state = normalizeState(dashboardGrid, {});
    applyLayout();
    saveState();
  }

  function bindButtons() {
    layoutEditBtn?.addEventListener("click", () => {
      setEditMode(!ctx.state.layoutEditMode);
    });

    layoutPanelsBtn?.addEventListener("click", togglePanelPicker);
    layoutResetBtn?.addEventListener("click", resetLayout);
    layoutDoneBtn?.addEventListener("click", () => setEditMode(false));
    layoutPanelPickerCloseBtn?.addEventListener("click", closePanelPicker);
  }

  function bindOutsideClose() {
    document.addEventListener("click", (event) => {
      if (!pickerOpen) return;
      if (layoutPanelPicker?.contains(event.target)) return;
      if (layoutPanelsBtn?.contains(event.target)) return;
      closePanelPicker();
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closePanelPicker();
      }
    });
  }

  function bindWindowResize() {
    window.addEventListener("resize", () => {
      applyLayout();
    });
  }

  return {
    setEditMode,

    init() {
      if (!dashboardGrid) return;

      installPerPanelFeatures();
      bindButtons();
      bindOutsideClose();
      bindWindowResize();

      state = normalizeState(
        dashboardGrid,
        ctx.store.getJSON(STORAGE_KEYS.layout, state)
      );

      setEditMode(false);
      applyLayout();
    },
  };
}