(function () {
  "use strict";

  const fileBrowser = window.GVYFileBrowser;

  const state = {
    status: "idle",
    error: "",
    data: null,
    limit: 1,
    offset: 0,
    picker: {
      open: false,
      status: "idle",
      error: "",
      currentPath: "",
      parentPath: "",
      entries: [],
      selection: "",
      filter: "",
    },
    fileModal: {
      open: false,
      status: "idle",
      error: "",
      file: "",
      data: null,
      limit: 100,
    },
  };

  const els = {
    pathInput: document.getElementById("error-report-path-input"),
    queryInput: document.getElementById("error-report-query-input"),
    fieldInput: document.getElementById("error-report-field-input"),
    browseButton: document.getElementById("error-report-browse-button"),
    status: document.getElementById("error-report-status"),
    message: document.getElementById("error-report-message"),
    content: document.getElementById("error-report-content"),
    pickerModal: document.getElementById("error-dir-picker-modal"),
    pickerBackdrop: document.getElementById("error-dir-picker-backdrop"),
    pickerCloseButton: document.getElementById("error-dir-picker-close-button"),
    pickerFilterInput: document.getElementById("error-dir-picker-filter-input"),
    pickerPathValue: document.getElementById("error-dir-picker-path-value"),
    pickerUpButton: document.getElementById("error-dir-picker-up-button"),
    pickerDirectories: document.getElementById("error-dir-picker-directories"),
    pickerSelect: document.getElementById("error-dir-picker-select"),
    pickerSelectionSummary: document.getElementById("error-dir-picker-selection-summary"),
    pickerCurrentButton: document.getElementById("error-dir-picker-current-button"),
    pickerChooseButton: document.getElementById("error-dir-picker-choose-button"),
    fileModal: document.getElementById("error-file-modal"),
    fileBackdrop: document.getElementById("error-file-backdrop"),
    fileCloseButton: document.getElementById("error-file-close-button"),
    fileTitle: document.getElementById("error-file-title"),
    fileContent: document.getElementById("error-file-content"),
  };

  function init() {
    const params = new URLSearchParams(window.location.search);
    setErrorPath(params.get("path") || "errors");
    els.browseButton.addEventListener("click", openDirectoryPicker);
    els.pickerBackdrop.addEventListener("click", closeDirectoryPicker);
    els.pickerCloseButton.addEventListener("click", closeDirectoryPicker);
    els.pickerFilterInput.addEventListener("input", function () {
      state.picker.filter = fileBrowser.normalizeFilter(els.pickerFilterInput.value);
      renderDirectoryPicker();
    });
    els.pickerUpButton.addEventListener("click", function () {
      loadDirectoryList(state.picker.parentPath || "");
    });
    els.pickerSelect.addEventListener("change", function () {
      state.picker.selection = els.pickerSelect.value || "";
      renderDirectoryPicker();
    });
    els.pickerSelect.addEventListener("dblclick", commitPickerSelection);
    els.pickerCurrentButton.addEventListener("click", commitCurrentDirectory);
    els.pickerChooseButton.addEventListener("click", commitPickerSelection);
    els.fileBackdrop.addEventListener("click", closeFileModal);
    els.fileCloseButton.addEventListener("click", closeFileModal);
    els.queryInput.addEventListener("keydown", submitOnEnter);
    els.fieldInput.addEventListener("keydown", submitOnEnter);
    render();
    loadErrorReport(0);
  }

  function submitOnEnter(event) {
    if (event.key === "Enter") {
      event.preventDefault();
      loadErrorReport(0);
    }
  }

  async function loadErrorReport(offset) {
    const path = cleanRelativePath(els.pathInput.value || "errors");
    setErrorPath(path);
    state.status = "loading";
    state.error = "";
    state.offset = Math.max(0, offset || 0);
    render();

    try {
      const params = new URLSearchParams();
      params.set("path", path);
      params.set("limit", String(state.limit));
      params.set("offset", String(state.offset));
      const query = els.queryInput.value.trim();
      const field = els.fieldInput.value.trim();
      if (query) {
        params.set("q", query);
      }
      if (field) {
        params.set("field", field);
      }

      const response = await fetch("/api/errors/report?" + params.toString());
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load error report");
      }
      state.status = "ok";
      state.data = payload;
      state.error = "";
      render();
    } catch (error) {
      state.status = "error";
      state.error = error.message || "Could not load error report";
      render();
    }
  }

  async function loadDirectoryList(path) {
    state.picker.status = "loading";
    state.picker.error = "";
    renderDirectoryPicker();

    try {
      const params = new URLSearchParams();
      params.set("kind", "parquet");
      if (path) {
        params.set("path", path);
      }
      const response = await fetch("/api/files?" + params.toString());
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load directories");
      }
      state.picker.status = "ok";
      state.picker.currentPath = payload.current_path || "";
      state.picker.parentPath = payload.parent_path || "";
      state.picker.entries = payload.entries || [];
      state.picker.error = "";
      renderDirectoryPicker();
    } catch (error) {
      state.picker.status = "error";
      state.picker.error = error.message || "Could not load directories";
      state.picker.entries = [];
      renderDirectoryPicker();
    }
  }

  async function loadFileReport(fileName) {
    state.fileModal.status = "loading";
    state.fileModal.error = "";
    state.fileModal.file = fileName;
    state.fileModal.data = null;
    renderFileModal();

    try {
      const params = new URLSearchParams();
      params.set("path", cleanRelativePath(els.pathInput.value || "errors"));
      params.set("file", fileName);
      params.set("limit", String(state.fileModal.limit));
      params.set("offset", "0");
      const query = els.queryInput.value.trim();
      const field = els.fieldInput.value.trim();
      if (query) {
        params.set("q", query);
      }
      if (field) {
        params.set("field", field);
      }

      const response = await fetch("/api/errors/report?" + params.toString());
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load error file");
      }
      state.fileModal.status = "ok";
      state.fileModal.data = payload;
      state.fileModal.error = "";
      renderFileModal();
    } catch (error) {
      state.fileModal.status = "error";
      state.fileModal.error = error.message || "Could not load error file";
      renderFileModal();
    }
  }

  async function parseJSON(response) {
    const text = await response.text();
    if (!text) {
      return null;
    }
    try {
      return JSON.parse(text);
    } catch (error) {
      throw new Error("Server returned invalid JSON");
    }
  }

  function render() {
    setBadge(els.status, statusText(), toneForStatus(state.status));
    els.message.textContent = state.error || "";
    if (state.error) {
      els.message.setAttribute("data-tone", "error");
    } else {
      els.message.removeAttribute("data-tone");
    }
    renderDirectoryPicker();
    renderFileModal();

    const report = state.data;
    if (!report) {
      els.content.innerHTML = '<p class="event-log__empty">Load an errors directory to see grouped fields, messages, files, and row samples.</p>';
      return;
    }

    els.content.innerHTML = [
      '<dl class="facts facts--compact error-facts">',
      factHTML("Files", compactNumber(report.file_count)),
      factHTML("Scanned rows", compactNumber(report.scanned_rows)),
      factHTML("Matched rows", compactNumber(report.matched_rows)),
      factHTML("Directory", valueText(report.relative_path || report.error_dir)),
      "</dl>",
      activeFilterHTML(),
      '<div class="error-report-grid">',
      errorBucketHTML("Problem columns", "Rows grouped by the column named in the validation error.", report.fields || [], "field"),
      errorMessageHTML("Error patterns", report.messages || []),
      errorBucketHTML("Source files", "Open a file to inspect its error rows.", report.files || [], "file"),
      "</div>",
    ].join("");

    els.content.querySelectorAll("[data-error-field]").forEach(function (button) {
      button.addEventListener("click", function () {
        els.fieldInput.value = button.getAttribute("data-error-field") || "";
        const query = button.getAttribute("data-error-query");
        if (query != null) {
          els.queryInput.value = query;
        }
        loadErrorReport(0);
      });
    });
    els.content.querySelectorAll("[data-error-query]").forEach(function (button) {
      if (button.hasAttribute("data-error-field")) {
        return;
      }
      button.addEventListener("click", function () {
        els.queryInput.value = button.getAttribute("data-error-query") || "";
        loadErrorReport(0);
      });
    });
    els.content.querySelectorAll("[data-error-file]").forEach(function (button) {
      button.addEventListener("click", function () {
        openFileModal(button.getAttribute("data-error-file") || "");
      });
    });
    els.content.querySelectorAll("[data-clear-filter]").forEach(function (button) {
      button.addEventListener("click", function () {
        const target = button.getAttribute("data-clear-filter");
        if (target === "field") {
          els.fieldInput.value = "";
        } else if (target === "query") {
          els.queryInput.value = "";
        } else {
          els.fieldInput.value = "";
          els.queryInput.value = "";
        }
        loadErrorReport(0);
      });
    });
  }

  function renderDirectoryPicker() {
    els.pickerModal.hidden = !state.picker.open;
    if (!state.picker.open) {
      return;
    }

    const directories = fileBrowser.filteredEntries(state.picker.entries, {
      filter: state.picker.filter,
      wantDirectory: true,
    });
    const currentPath = state.picker.currentPath || "";
    const selectedValue = state.picker.selection || "";

    els.pickerFilterInput.value = state.picker.filter || "";
    els.pickerPathValue.textContent = "/" + currentPath;
    els.pickerUpButton.disabled = !state.picker.parentPath && !currentPath;
    els.pickerCurrentButton.disabled = !currentPath || state.picker.status === "loading";
    els.pickerChooseButton.disabled = !selectedValue || state.picker.status === "loading";

    fileBrowser.renderDirectoryList(els.pickerDirectories, directories, {
      onChoose: function (path) {
        loadDirectoryList(path);
      },
      emptyText: state.picker.status === "loading" ? "Loading directories..." : "No subdirectories match the current filter.",
    });

    fileBrowser.populateSelect(els.pickerSelect, directories, {
      selectedValue: selectedValue,
      clearWhenEmptySelection: true,
      emptyText: state.picker.status === "loading" ? "Loading directories..." : "No subdirectories match the current filter",
    });

    if (state.picker.error) {
      els.pickerSelectionSummary.textContent = state.picker.error;
      els.pickerSelectionSummary.setAttribute("data-tone", "error");
      return;
    }
    els.pickerSelectionSummary.removeAttribute("data-tone");
    els.pickerSelectionSummary.textContent = selectedValue ? selectedValue : currentPath ? "Current: " + currentPath : "No directory selected.";
  }

  function openDirectoryPicker() {
    const currentValue = cleanRelativePath(els.pathInput.value || "errors");
    state.picker.open = true;
    state.picker.selection = currentValue;
    state.picker.filter = "";
    state.picker.error = "";
    loadDirectoryList(currentValue ? dirName(currentValue) : "");
    renderDirectoryPicker();
    window.setTimeout(function () {
      els.pickerFilterInput.focus();
    }, 0);
  }

  function closeDirectoryPicker() {
    state.picker.open = false;
    renderDirectoryPicker();
  }

  function commitCurrentDirectory() {
    const value = cleanRelativePath(state.picker.currentPath || "");
    if (!value) {
      return;
    }
    applyDirectorySelection(value);
  }

  function commitPickerSelection() {
    const value = cleanRelativePath(state.picker.selection || "");
    if (!value) {
      return;
    }
    applyDirectorySelection(value);
  }

  function applyDirectorySelection(value) {
    setErrorPath(value);
    closeDirectoryPicker();
    loadErrorReport(0);
  }

  function setErrorPath(value) {
    const path = cleanRelativePath(value || "errors") || "errors";
    els.pathInput.value = path;
    els.browseButton.title = "Selected: " + path;
  }

  function openFileModal(fileName) {
    if (!fileName) {
      return;
    }
    state.fileModal.open = true;
    state.fileModal.file = fileName;
    state.fileModal.data = null;
    state.fileModal.error = "";
    loadFileReport(fileName);
    renderFileModal();
  }

  function closeFileModal() {
    state.fileModal.open = false;
    renderFileModal();
  }

  function renderFileModal() {
    els.fileModal.hidden = !state.fileModal.open;
    if (!state.fileModal.open) {
      return;
    }

    const fileName = state.fileModal.file || "Error file";
    els.fileTitle.textContent = fileName;
    if (state.fileModal.status === "loading") {
      els.fileContent.innerHTML = '<p class="event-log__empty">Loading file rows...</p>';
      return;
    }
    if (state.fileModal.error) {
      els.fileContent.innerHTML = '<p class="event-log__empty">' + escapeHTML(state.fileModal.error) + "</p>";
      return;
    }

    const report = state.fileModal.data;
    const samples = report && Array.isArray(report.samples) ? report.samples : [];
    if (!report || !samples.length) {
      els.fileContent.innerHTML = '<p class="event-log__empty">No matching rows in this file.</p>';
      return;
    }

    els.fileContent.innerHTML = [
      '<div class="error-file-rows">',
      samples.map(fileRowHTML).join(""),
      "</div>",
    ].join("");
  }

  function statusText() {
    switch (state.status) {
      case "loading":
        return "Loading";
      case "ok":
        return "Loaded";
      case "error":
        return "Failed";
      default:
        return "Not loaded";
    }
  }

  function toneForStatus(status) {
    switch (status) {
      case "loading":
        return "info";
      case "ok":
        return "ok";
      case "error":
        return "error";
      default:
        return "muted";
    }
  }

  function setBadge(element, text, tone) {
    element.textContent = text;
    element.className = "badge badge--" + tone;
  }

  function factHTML(label, value) {
    return '<div class="fact"><dt>' + escapeHTML(label) + "</dt><dd>" + escapeHTML(valueText(value)) + "</dd></div>";
  }

  function activeFilterHTML() {
    const field = els.fieldInput.value.trim();
    const query = els.queryInput.value.trim();
    if (!field && !query) {
      return "";
    }
    return [
      '<div class="active-filters">',
      '<span class="active-filters__label">Active filters</span>',
      field ? '<button class="filter-pill" type="button" data-clear-filter="field"><span>Column: ' + escapeHTML(field) + "</span><strong>Clear</strong></button>" : "",
      query ? '<button class="filter-pill" type="button" data-clear-filter="query"><span>Search: ' + escapeHTML(query) + "</span><strong>Clear</strong></button>" : "",
      '<button class="button button--secondary button--small" type="button" data-clear-filter="all">Clear all</button>',
      "</div>",
    ].join("");
  }

  function errorBucketHTML(title, note, buckets, mode) {
    if (!buckets.length) {
      return cardHTML(title, [rowHTML("Matches", "0")]);
    }
    return [
      '<section class="summary-card error-card">',
      "<h3>" + escapeHTML(title) + "</h3>",
      '<p class="error-card__note">' + escapeHTML(note) + "</p>",
      '<ol class="error-bucket-list' + (mode === "file" ? " error-bucket-list--files" : "") + '">',
      buckets.map(function (bucket) {
        const attr = mode === "field" ? ' data-error-field="' + escapeAttr(bucket.name) + '"' : ' data-error-file="' + escapeAttr(bucket.name) + '"';
        const label = mode === "field" ? "Filter column" : "Open file";
        return '<li><button class="error-chip" type="button"' + attr + '><span><em>' + escapeHTML(label) + "</em>" + escapeHTML(bucket.name) + '</span><strong>' + compactNumber(bucket.count) + "</strong></button></li>";
      }).join(""),
      "</ol>",
      "</section>",
    ].join("");
  }

  function errorMessageHTML(title, messages) {
    if (!messages.length) {
      return cardHTML(title, [rowHTML("Matches", "0")]);
    }
    return [
      '<section class="summary-card error-card error-card--wide">',
      "<h3>" + escapeHTML(title) + "</h3>",
      '<p class="error-card__note">Rows grouped by the exact validation message. Selecting one applies the column and message filters together.</p>',
      '<ol class="error-message-list">',
      messages.map(function (item) {
        const query = queryForPattern(item.message || "");
        return [
          "<li>",
          '<button class="error-message-row" type="button" data-error-field="' + escapeAttr(item.field) + '" data-error-query="' + escapeAttr(query) + '">',
          "<span><strong>" + escapeHTML(item.field) + "</strong><small>Filter this error pattern</small>" + escapeHTML(item.message || "Validation error") + "</span>",
          "<em>" + compactNumber(item.count) + "</em>",
          "</button>",
          "</li>",
        ].join("");
      }).join(""),
      "</ol>",
      "</section>",
    ].join("");
  }

  function queryForPattern(message) {
    const clean = String(message || "").replace(/<value>/g, "").replace(/\s+/g, " ").trim();
    return clean.replace(/:\s*$/, "").trim();
  }

  function errorSampleHTML(sample) {
    const columns = sampleColumns(sample);
    const erroredColumns = columns.filter(function (column) {
      return column.errored;
    });
    const errorFieldRows = erroredColumns.length ? erroredColumns.map(errorColumnHTML).join("") : '<p class="event-log__empty">No column-specific error fields were found for this row.</p>';
    const allColumnRows = columns.length ? columns.map(errorColumnHTML).join("") : '<p class="event-log__empty">No row columns available.</p>';
    return [
      '<article class="error-sample">',
      '<div class="error-sample__head">',
      "<strong>" + escapeHTML(sample.file || "error file") + "</strong>",
      "<span>Row " + escapeHTML(sample.row_number || "unknown") + "</span>",
      "</div>",
      '<p class="error-sample__message">' + escapeHTML(sample.errors || "Validation error") + "</p>",
      '<section class="error-sample__error-fields">',
      "<h4>Error columns</h4>",
      '<dl class="error-column-list">' + errorFieldRows + "</dl>",
      "</section>",
      '<details class="error-sample__all-columns">',
      "<summary>All row columns</summary>",
      '<dl class="error-column-grid">' + allColumnRows + "</dl>",
      "</details>",
      "</article>",
    ].join("");
  }

  function fileRowHTML(sample) {
    const columns = sampleColumns(sample);
    const allColumnRows = columns.length ? columns.map(errorColumnHTML).join("") : '<p class="event-log__empty">No row columns available.</p>';
    return [
      '<article class="error-file-row">',
      '<div class="error-file-row__head">',
      "<strong>Row " + escapeHTML(sample.row_number || "unknown") + "</strong>",
      '<span class="error-file-row__message">' + escapeHTML(sample.errors || "Validation error") + "</span>",
      "</div>",
      '<dl class="error-column-grid">' + allColumnRows + "</dl>",
      "</article>",
    ].join("");
  }

  function sampleColumns(sample) {
    if (Array.isArray(sample.columns) && sample.columns.length) {
      return sample.columns.map(function (column) {
        return {
          name: column.name || "",
          value: column.value == null ? "" : String(column.value),
          errored: Boolean(column.errored),
        };
      });
    }
    const values = sample.values || {};
    const errorFields = new Set(Array.isArray(sample.error_fields) ? sample.error_fields : []);
    return Object.keys(values).map(function (key) {
      return {
        name: key,
        value: values[key],
        errored: errorFields.has(key),
      };
    });
  }

  function errorColumnHTML(column) {
    return [
      '<div class="error-column-item' + (column.errored ? " error-column-item--errored" : "") + '">',
      "<dt>" + escapeHTML(column.name) + "</dt>",
      "<dd>" + escapeHTML(displayCellValue(column.value)) + "</dd>",
      "</div>",
    ].join("");
  }

  function displayCellValue(value) {
    if (value == null || value === "") {
      return "(blank)";
    }
    return String(value);
  }

  function cardHTML(title, rows) {
    return [
      '<section class="summary-card">',
      "<h3>" + escapeHTML(title) + "</h3>",
      '<dl class="metric-list">',
      rows.join(""),
      "</dl>",
      "</section>",
    ].join("");
  }

  function rowHTML(label, value) {
    return "<div><dt>" + escapeHTML(label) + "</dt><dd>" + escapeHTML(valueText(value)) + "</dd></div>";
  }

  function valueText(value) {
    if (value == null || value === "") {
      return "Not available";
    }
    return String(value);
  }

  function compactNumber(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "0";
    }
    const absolute = Math.abs(number);
    const units = [
      { value: 1000000000, suffix: "B" },
      { value: 1000000, suffix: "M" },
      { value: 1000, suffix: "K" },
    ];
    for (let index = 0; index < units.length; index += 1) {
      const unit = units[index];
      if (absolute >= unit.value) {
        return trimTrailingDecimal(number / unit.value) + unit.suffix;
      }
    }
    return String(Math.round(number));
  }

  function trimTrailingDecimal(value) {
    return value.toFixed(1).replace(/\.0$/, "");
  }

  function cleanRelativePath(path) {
    const value = String(path || "").replace(/\\/g, "/").replace(/^\/+/, "");
    const parts = [];
    value.split("/").forEach(function (part) {
      if (!part || part === ".") {
        return;
      }
      if (part === "..") {
        parts.pop();
        return;
      }
      parts.push(part);
    });
    return parts.join("/");
  }

  function dirName(path) {
    const clean = cleanRelativePath(path);
    const index = clean.lastIndexOf("/");
    return index >= 0 ? clean.slice(0, index) : "";
  }

  function escapeHTML(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function escapeAttr(value) {
    return escapeHTML(value);
  }

  init();
})();
