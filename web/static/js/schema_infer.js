(function () {
  "use strict";

  const fileBrowser = window.GVYFileBrowser;

  const state = {
    currentPath: "",
    parentPath: "",
    entries: [],
    selectedCSV: "",
    pickerSelection: "",
    filter: "",
    result: null,
    activeView: "fields",
    savingSchema: false,
  };

  const els = {
    refreshFilesButton: document.getElementById("refresh-files-button"),
    csvOpenButton: document.getElementById("csv-open-button"),
    csvSelectionSummary: document.getElementById("csv-selection-summary"),
    sampleSizeInput: document.getElementById("sample-size-input"),
    strategyInput: document.getElementById("strategy-input"),
    keepSamplesInput: document.getElementById("keep-samples-input"),
    writeParquetInput: document.getElementById("write-parquet-input"),
    runInferenceButton: document.getElementById("run-inference-button"),
    openEditorPrimaryButton: document.getElementById("open-editor-primary-button"),
    inferMessage: document.getElementById("infer-message"),
    resultNote: document.getElementById("result-note"),
    durationBadge: document.getElementById("duration-badge"),
    sampledRowsValue: document.getElementById("sampled-rows-value"),
    fileSizeValue: document.getElementById("file-size-value"),
    sampleParquetValue: document.getElementById("sample-parquet-value"),
    fieldsTableBody: document.getElementById("fields-table-body"),
    schemaJSONOutput: document.getElementById("schema-json-output"),
    openEditorButton: document.getElementById("open-editor-button"),
    copySchemaButton: document.getElementById("copy-schema-button"),
    samplesOutput: document.getElementById("samples-output"),
    warningsList: document.getElementById("warnings-list"),
    tabs: Array.from(document.querySelectorAll("[data-view]")),
    views: Array.from(document.querySelectorAll(".infer-view")),
    pickerModal: document.getElementById("csv-picker-modal"),
    pickerBackdrop: document.getElementById("picker-backdrop"),
    pickerCloseButton: document.getElementById("picker-close-button"),
    pickerFilterInput: document.getElementById("picker-filter-input"),
    currentPath: document.getElementById("current-path"),
    upButton: document.getElementById("up-button"),
    directoryList: document.getElementById("directory-list"),
    csvFileSelect: document.getElementById("csv-file-select"),
    pickerSelectionSummary: document.getElementById("picker-selection-summary"),
    pickerChooseButton: document.getElementById("picker-choose-button"),
  };

  function init() {
    bindEvents();
    render();
    applyStartupIntent();
  }

  function bindEvents() {
    els.refreshFilesButton.addEventListener("click", function () {
      loadFileList(state.currentPath);
    });
    els.csvOpenButton.addEventListener("click", openPicker);
    els.pickerBackdrop.addEventListener("click", closePicker);
    els.pickerCloseButton.addEventListener("click", closePicker);
    els.pickerFilterInput.addEventListener("input", function () {
      state.filter = fileBrowser.normalizeFilter(els.pickerFilterInput.value);
      renderPicker();
    });
    els.upButton.addEventListener("click", function () {
      loadFileList(state.parentPath || "");
    });
    els.csvFileSelect.addEventListener("change", function () {
      state.pickerSelection = els.csvFileSelect.value || "";
      renderPicker();
    });
    els.csvFileSelect.addEventListener("dblclick", choosePickerSelection);
    els.pickerChooseButton.addEventListener("click", choosePickerSelection);
    els.runInferenceButton.addEventListener("click", runInference);
    els.openEditorPrimaryButton.addEventListener("click", saveAndOpenSchemaEditor);
    els.openEditorButton.addEventListener("click", saveAndOpenSchemaEditor);
    els.copySchemaButton.addEventListener("click", copySchemaJSON);
    els.tabs.forEach(function (tab) {
      tab.addEventListener("click", function () {
        state.activeView = tab.getAttribute("data-view") || "fields";
        renderTabs();
      });
    });
  }

  function applyStartupIntent() {
    const params = new URLSearchParams(window.location.search || "");
    const csvPath = fileBrowser.cleanRelativePath(params.get("csv") || "");
    if (!csvPath) {
      loadFileList("");
      return;
    }
    state.selectedCSV = csvPath;
    state.pickerSelection = csvPath;
    render();
    loadFileList(fileBrowser.dirName(csvPath));
  }

  async function loadFileList(path) {
    try {
      const params = new URLSearchParams();
      params.set("kind", "csv");
      if (path) {
        params.set("path", path);
      }
      const response = await fetch("/api/files?" + params.toString());
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load CSV files");
      }
      state.currentPath = payload.current_path || "";
      state.parentPath = payload.parent_path || "";
      state.entries = payload.entries || [];
      renderPicker();
    } catch (err) {
      setMessage(err.message || String(err), "error");
    }
  }

  function openPicker() {
    state.pickerSelection = state.selectedCSV;
    state.filter = "";
    els.pickerFilterInput.value = "";
    els.pickerModal.hidden = false;
    renderPicker();
    window.requestAnimationFrame(function () {
      els.pickerFilterInput.focus();
    });
  }

  function closePicker() {
    els.pickerModal.hidden = true;
  }

  function choosePickerSelection() {
    if (!state.pickerSelection) {
      setMessage("Choose a CSV file first.", "warn");
      return;
    }
    state.selectedCSV = state.pickerSelection;
    state.result = null;
    closePicker();
    setMessage("Selected " + state.selectedCSV, "ok");
    render();
  }

  async function runInference() {
    if (!state.selectedCSV) {
      setMessage("Select a CSV file first.", "warn");
      return;
    }
    const sampleSize = Number.parseInt(els.sampleSizeInput.value, 10);
    if (!Number.isFinite(sampleSize) || sampleSize < 1) {
      setMessage("Sample size must be at least 1.", "warn");
      return;
    }

    setBusy(true);
    setMessage("Running schema inference.", "info");
    try {
      const response = await fetch("/api/schema/infer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          csv_path: state.selectedCSV,
          sample_size: sampleSize,
          strategy: els.strategyInput.value || "byte-spread",
          keep_samples: els.keepSamplesInput.checked,
          write_sample_parquet: els.writeParquetInput.checked,
        }),
      });
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Schema inference failed");
      }
      state.result = payload;
      state.activeView = "fields";
      setMessage("Inference complete.", "ok");
      render();
    } catch (err) {
      setMessage(err.message || String(err), "error");
    } finally {
      setBusy(false);
    }
  }

  async function copySchemaJSON() {
    const text = els.schemaJSONOutput.textContent || "{}";
    try {
      await navigator.clipboard.writeText(text);
      setMessage("Schema JSON copied.", "ok");
    } catch (err) {
      setMessage("Could not copy schema JSON.", "warn");
    }
  }

  async function saveAndOpenSchemaEditor() {
    const inference = state.result && state.result.inference;
    const schema = inference && inference.schema;
    if (!schema || !Array.isArray(schema.fields) || !schema.fields.length) {
      setMessage("Run inference before opening the schema editor.", "warn");
      return;
    }

    const path = inferredSchemaPath();
    if (!path) {
      setMessage("Could not build an inferred schema path.", "error");
      return;
    }

    state.savingSchema = true;
    renderResult();
    setMessage("Saving inferred schema draft.", "info");
    try {
      const response = await fetch("/api/schema", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          path: path,
          schema: schema,
        }),
      });
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not save inferred schema");
      }
      const relativePath = payload.relative_path || path;
      setMessage("Opening inferred schema in editor.", "ok");
      openSchemaEditor(relativePath);
    } catch (err) {
      setMessage(err.message || String(err), "error");
    } finally {
      state.savingSchema = false;
      renderResult();
    }
  }

  function render() {
    els.csvSelectionSummary.textContent = state.selectedCSV || "No CSV selected";
    renderResult();
    renderTabs();
  }

  function renderPicker() {
    const directories = fileBrowser.filteredEntries(state.entries, {
      filter: state.filter,
      wantDirectory: true,
    });
    const files = fileBrowser.filteredEntries(state.entries, {
      filter: state.filter,
      wantDirectory: false,
    });

    els.currentPath.textContent = state.currentPath ? "/" + state.currentPath : "/";
    els.upButton.disabled = !state.parentPath && !state.currentPath;
    fileBrowser.renderDirectoryList(els.directoryList, directories, {
      emptyText: "No subdirectories here.",
      onChoose: loadFileList,
    });
    fileBrowser.populateSelect(els.csvFileSelect, files, {
      selectedValue: state.pickerSelection,
      clearWhenEmptySelection: true,
      emptyText: "No CSV files here",
      textFor: function (entry) {
        return entry.relative_path + " (" + formatBytes(entry.size_bytes || 0) + ")";
      },
    });
    els.pickerSelectionSummary.textContent = state.pickerSelection
      ? "Selected: " + state.pickerSelection
      : "No file selected.";
    els.pickerChooseButton.disabled = !state.pickerSelection;
  }

  function renderResult() {
    const response = state.result;
    const inference = response && response.inference;
    if (!inference) {
      els.resultNote.textContent = "Run inference to see detected fields.";
      els.durationBadge.textContent = "Not run";
      els.durationBadge.className = "badge badge--muted";
      els.sampledRowsValue.textContent = "-";
      els.fileSizeValue.textContent = "-";
      els.sampleParquetValue.textContent = "-";
      els.fieldsTableBody.innerHTML = '<tr><td colspan="6">No inference result yet.</td></tr>';
      els.schemaJSONOutput.textContent = "{}";
      els.openEditorButton.disabled = true;
      els.openEditorPrimaryButton.disabled = true;
      els.samplesOutput.textContent = "No retained samples yet.";
      els.warningsList.innerHTML = "<li>No warnings yet.</li>";
      return;
    }

    els.resultNote.textContent = (response.csv_relative_path || state.selectedCSV) + " using " + inference.strategy;
    els.durationBadge.textContent = String(inference.duration_millis || 0) + " ms";
    els.durationBadge.className = "badge badge--ok";
    els.sampledRowsValue.textContent = String(inference.sampled_rows || 0);
    els.fileSizeValue.textContent = formatBytes(inference.file_size_bytes || 0);
    els.sampleParquetValue.textContent = response.sample_parquet_relative_path || "-";
    renderFields(inference.fields || []);
    els.schemaJSONOutput.textContent = JSON.stringify(inference.schema || { fields: [] }, null, 2);
    els.openEditorButton.disabled = state.savingSchema;
    els.openEditorPrimaryButton.disabled = state.savingSchema;
    els.openEditorButton.textContent = state.savingSchema ? "Saving" : "Open in editor";
    els.openEditorPrimaryButton.textContent = state.savingSchema ? "Saving" : "Open in schema editor";
    renderSamples(inference.samples || []);
    renderWarnings(inference.warnings || []);
  }

  function renderFields(fields) {
    if (!fields.length) {
      els.fieldsTableBody.innerHTML = '<tr><td colspan="6">No fields inferred.</td></tr>';
      return;
    }
    els.fieldsTableBody.innerHTML = fields.map(function (field) {
      return "<tr>" +
        "<td><strong>" + escapeHTML(field.name) + "</strong><span>" + escapeHTML(field.parquet_name || "") + "</span></td>" +
        "<td><span class=\"type-pill\">" + escapeHTML(field.type || "string") + "</span></td>" +
        "<td>" + (field.required ? "yes" : "no") + "</td>" +
        "<td>" + formatPercent(field.confidence) + "</td>" +
        "<td>" + String(field.blank_count || 0) + "</td>" +
        "<td>" + escapeHTML((field.sample_values || []).join(", ")) + "</td>" +
        "</tr>";
    }).join("");
  }

  function renderSamples(samples) {
    if (!samples.length) {
      els.samplesOutput.textContent = "No retained samples in this result.";
      return;
    }
    els.samplesOutput.innerHTML = samples.map(function (sample) {
      const values = sample.values || {};
      const pairs = Object.keys(values).map(function (key) {
        return "<dt>" + escapeHTML(key) + "</dt><dd>" + escapeHTML(values[key]) + "</dd>";
      }).join("");
      return "<article class=\"sample-row\"><header>Sample " + sample.sample_index + "<span>byte " + sample.offset_end + "</span></header><dl>" + pairs + "</dl></article>";
    }).join("");
  }

  function renderWarnings(warnings) {
    if (!warnings.length) {
      els.warningsList.innerHTML = "<li>No warnings.</li>";
      return;
    }
    els.warningsList.innerHTML = warnings.map(function (warning) {
      return "<li>" + escapeHTML(warning) + "</li>";
    }).join("");
  }

  function renderTabs() {
    els.tabs.forEach(function (tab) {
      const active = tab.getAttribute("data-view") === state.activeView;
      tab.classList.toggle("infer-tab--active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
    });
    els.views.forEach(function (view) {
      view.classList.toggle("infer-view--active", view.id === state.activeView + "-view");
    });
  }

  function setBusy(busy) {
    els.runInferenceButton.disabled = busy;
    els.runInferenceButton.textContent = busy ? "Running" : "Run inference";
  }

  function setMessage(message, tone) {
    els.inferMessage.textContent = message || "";
    els.inferMessage.dataset.tone = tone || "";
  }

  function openSchemaEditor(path) {
    const clean = fileBrowser.cleanRelativePath(path);
    if (!clean) {
      return;
    }
    if (window.parent && window.parent !== window) {
      try {
        if (typeof window.parent.GVYOpenInferredSchema === "function") {
          window.parent.GVYOpenInferredSchema(clean);
          return;
        }
      } catch (error) {
        // Fall back to postMessage below.
      }
      window.parent.postMessage({
        type: "gvy:schema-infer-open-schema",
        path: clean,
      }, window.location.origin);
      return;
    }
    const params = new URLSearchParams();
    params.set("path", clean);
    window.location.href = "/schema-editor?" + params.toString();
  }

  function inferredSchemaPath() {
    const csvPath = fileBrowser.cleanRelativePath(state.selectedCSV);
    if (!csvPath) {
      return "";
    }
    const slug = snakeCaseStem(fileBrowser.baseName(csvPath).replace(/\.csv$/i, "")) || "inferred";
    return "runs/" + slug + "/schema.json";
  }

  function snakeCaseStem(value) {
    return String(value || "")
      .trim()
      .replace(/[^A-Za-z0-9]+/g, "_")
      .replace(/[A-Z]/g, function (match) { return match.toLowerCase(); })
      .replace(/^_+|_+$/g, "");
  }

  async function parseJSON(response) {
    const text = await response.text();
    if (!text) {
      return null;
    }
    try {
      return JSON.parse(text);
    } catch (err) {
      throw new Error("Server returned invalid JSON");
    }
  }

  function formatBytes(bytes) {
    const n = Number(bytes || 0);
    if (n < 1024) {
      return String(n) + " B";
    }
    const units = ["KB", "MB", "GB", "TB"];
    let value = n / 1024;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }
    return value.toFixed(value >= 10 ? 1 : 2) + " " + units[unitIndex];
  }

  function formatPercent(value) {
    const n = Number(value || 0);
    return Math.round(n * 100) + "%";
  }

  function escapeHTML(value) {
    return fileBrowser.escapeHTML(value);
  }

  init();
})();
