(function () {
  "use strict";

  const bootstrap = window.GVY_UI_BOOTSTRAP || {};
  const state = {
    health: bootstrap.server || { status: "ok", busy: false, version: "v1", working_root: "" },
    runId: bootstrap.latest_run_id || "",
    snapshot: null,
    result: null,
    files: {
      csv: [],
      schema: [],
    },
    filters: {
      csv: "",
      schema: "",
    },
    selected: {
      csv: "",
      schema: "",
    },
    events: [],
    seenEvents: new Set(),
    stream: null,
    pendingRefresh: 0,
  };

  const els = {
    form: document.getElementById("run-form"),
    refreshFilesButton: document.getElementById("refresh-files-button"),
    csvFilterInput: document.getElementById("csv-filter-input"),
    schemaFilterInput: document.getElementById("schema-filter-input"),
    csvSelect: document.getElementById("csv-select"),
    schemaSelect: document.getElementById("schema-select"),
    csvCount: document.getElementById("csv-count"),
    schemaCount: document.getElementById("schema-count"),
    csvSelectionSummary: document.getElementById("csv-selection-summary"),
    schemaSelectionSummary: document.getElementById("schema-selection-summary"),
    selectedCSVValue: document.getElementById("selected-csv-value"),
    selectedSchemaValue: document.getElementById("selected-schema-value"),
    submitButton: document.getElementById("submit-button"),
    refreshButton: document.getElementById("refresh-button"),
    formMessage: document.getElementById("form-message"),
    serverStatusText: document.getElementById("server-status-text"),
    serverStatusBadge: document.getElementById("server-status-badge"),
    runStateBadge: document.getElementById("run-state-badge"),
    runIDValue: document.getElementById("run-id-value"),
    runPhaseValue: document.getElementById("run-phase-value"),
    runProgressValue: document.getElementById("run-progress-value"),
    workspaceValue: document.getElementById("workspace-value"),
    phaseHeading: document.getElementById("phase-heading"),
    phaseDetail: document.getElementById("phase-detail"),
    progressFill: document.getElementById("progress-fill"),
    summaryCards: document.getElementById("summary-cards"),
    eventLog: document.getElementById("event-log"),
  };

  function init() {
    bindEvents();
    render();
    refreshHealth();
    refreshFileLists();
    if (state.runId) {
      syncRun(state.runId);
    }
  }

  function bindEvents() {
    els.refreshFilesButton.addEventListener("click", function () {
      refreshFileLists();
    });

    els.csvFilterInput.addEventListener("input", function () {
      state.filters.csv = els.csvFilterInput.value.trim().toLowerCase();
      renderFileSelect("csv");
    });

    els.schemaFilterInput.addEventListener("input", function () {
      state.filters.schema = els.schemaFilterInput.value.trim().toLowerCase();
      renderFileSelect("schema");
    });

    els.csvSelect.addEventListener("change", function () {
      state.selected.csv = els.csvSelect.value;
      clearFormMessage();
      render();
    });

    els.schemaSelect.addEventListener("change", function () {
      state.selected.schema = els.schemaSelect.value;
      clearFormMessage();
      render();
    });

    els.refreshButton.addEventListener("click", function () {
      refreshHealth();
      if (state.runId) {
        syncRun(state.runId);
      }
    });

    els.form.addEventListener("submit", function (event) {
      event.preventDefault();
      submitRun();
    });
  }

  async function refreshFileLists() {
    clearFormMessage();
    await Promise.all([loadFileList("csv"), loadFileList("schema")]);
    render();
  }

  async function loadFileList(kind) {
    updateFileCount(kind, "Loading…");
    try {
      const response = await fetch("/api/files?kind=" + encodeURIComponent(kind));
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load " + kind + " files");
      }
      state.files[kind] = payload.files || [];
      if (!hasRelativePath(kind, state.selected[kind])) {
        state.selected[kind] = "";
      }
      renderFileSelect(kind);
    } catch (error) {
      state.files[kind] = [];
      renderFileSelect(kind);
      setFormMessage(error.message || "Could not load file lists", "error");
    }
  }

  async function submitRun() {
    if (!state.selected.csv || !state.selected.schema) {
      setFormMessage("Choose both a CSV file and a schema JSON file before starting a run.", "warn");
      return;
    }

    closeStream();
    els.submitButton.disabled = true;
    setFormMessage("Creating validation run from the server working directory…", "info");

    try {
      const response = await fetch("/api/runs", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          csv_path: state.selected.csv,
          schema_path: state.selected.schema,
        }),
      });
      const payload = await parseJSON(response);

      if (response.status === 409 && payload && payload.error_code === "BUSY") {
        state.health.busy = true;
        setFormMessage("The server is already running another validation. The current run remains inspectable until it finishes.", "warn");
        if (state.health.latest_run_id) {
          syncRun(state.health.latest_run_id);
        }
        render();
        return;
      }

      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Run creation failed");
      }

      state.runId = payload.run.run_id;
      state.result = null;
      state.health.busy = true;
      replaceSnapshot(payload.run);
      openStream(state.runId);
      render();
      setFormMessage("Run created. Streaming progress now.", "ok");
      refreshHealth();
    } catch (error) {
      setFormMessage(error.message || "Run creation failed", "error");
    } finally {
      render();
    }
  }

  async function refreshHealth() {
    try {
      const response = await fetch("/health");
      if (!response.ok) {
        return;
      }
      state.health = await response.json();
      render();
    } catch (error) {
      setFormMessage("Health check failed. The page will keep showing the last known state.", "warn");
    }
  }

  async function syncRun(runId) {
    if (!runId) {
      return;
    }

    try {
      const response = await fetch("/api/runs/" + encodeURIComponent(runId));
      if (!response.ok) {
        if (response.status === 404) {
          state.runId = "";
          state.snapshot = null;
          state.result = null;
          state.events = [];
          state.seenEvents = new Set();
          closeStream();
          render();
          return;
        }
        throw new Error("Could not fetch the latest run snapshot");
      }

      const payload = await response.json();
      replaceSnapshot(payload.run);
      applySelectionFromSnapshot(payload.run);
      render();

      if (isTerminalState(payload.run.state)) {
        closeStream();
        await fetchResult(runId);
      } else {
        openStream(runId);
      }
    } catch (error) {
      setFormMessage(error.message || "Could not refresh the run snapshot", "error");
    }
  }

  async function fetchResult(runId) {
    try {
      const response = await fetch("/api/runs/" + encodeURIComponent(runId) + "/result");
      if (!response.ok) {
        if (response.status === 409) {
          return;
        }
        throw new Error("Could not fetch the final run result");
      }
      state.result = await response.json();
      state.health.busy = false;
      render();
      refreshHealth();
    } catch (error) {
      setFormMessage(error.message || "Could not fetch the final run result", "error");
    }
  }

  function replaceSnapshot(snapshot) {
    state.snapshot = snapshot;
    state.runId = snapshot.run_id;
    if (!isTerminalState(snapshot.state)) {
      state.result = null;
    }
    state.events = [];
    state.seenEvents = new Set();
    (snapshot.events || []).forEach(function (event) {
      pushEvent(event);
    });
  }

  function applySelectionFromSnapshot(snapshot) {
    if (!snapshot || !snapshot.workspace) {
      return;
    }
    const csvPath = toRelativePath(snapshot.workspace.input_csv_path);
    const schemaPath = toRelativePath(snapshot.workspace.schema_path);
    if (csvPath) {
      state.selected.csv = csvPath;
    }
    if (schemaPath) {
      state.selected.schema = schemaPath;
    }
  }

  function pushEvent(event) {
    const key = [
      event.time || "",
      event.phase || "",
      event.type || "",
      event.message || "",
      event.percent == null ? "" : String(event.percent),
      stableStringify(event.metrics || {}),
    ].join("|");
    if (state.seenEvents.has(key)) {
      return;
    }
    state.seenEvents.add(key);
    state.events.push(event);
    if (state.events.length > 60) {
      state.events = state.events.slice(state.events.length - 60);
    }
  }

  function openStream(runId) {
    if (!runId || (state.snapshot && isTerminalState(state.snapshot.state))) {
      return;
    }
    if (state.stream && state.stream.runId === runId) {
      return;
    }

    closeStream();
    const source = new EventSource("/api/runs/" + encodeURIComponent(runId) + "/events");
    source.runId = runId;
    source.addEventListener("progress", function (message) {
      try {
        const event = JSON.parse(message.data);
        pushEvent(event);
        if (state.snapshot) {
          state.snapshot.latest_event = event;
          if (event.type === "completed") {
            state.snapshot.state = "completed";
          } else if (event.type === "failed") {
            state.snapshot.state = "failed";
          }
        }
        render();
      } catch (error) {
        setFormMessage("Received an unreadable progress event from the server.", "error");
      }
    });
    source.onerror = function () {
      if (state.stream !== source) {
        return;
      }
      closeStream();
      if (state.snapshot && !isTerminalState(state.snapshot.state)) {
        queueRefresh(runId, 900);
      } else if (state.snapshot) {
        fetchResult(runId);
      }
    };
    state.stream = source;
  }

  function queueRefresh(runId, delayMs) {
    window.clearTimeout(state.pendingRefresh);
    state.pendingRefresh = window.setTimeout(function () {
      syncRun(runId);
    }, delayMs);
  }

  function closeStream() {
    if (state.stream) {
      state.stream.close();
      state.stream = null;
    }
    window.clearTimeout(state.pendingRefresh);
    state.pendingRefresh = 0;
  }

  function render() {
    renderServerState();
    renderFileSelect("csv");
    renderFileSelect("schema");
    renderSelections();
    renderRunState();
    renderSummary();
    renderEvents();
    updateSubmitState();
  }

  function renderServerState() {
    const busy = Boolean(state.health && state.health.busy);
    els.serverStatusText.textContent = busy ? "Busy" : "Idle";
    setBadge(els.serverStatusBadge, busy ? "Single run occupied" : "Ready for a new run", busy ? "warn" : "ok");
  }

  function renderFileSelect(kind) {
    const select = kind === "csv" ? els.csvSelect : els.schemaSelect;
    const summary = kind === "csv" ? els.csvSelectionSummary : els.schemaSelectionSummary;
    const files = filteredFiles(kind);

    select.innerHTML = "";
    if (!files.length) {
      const option = document.createElement("option");
      option.disabled = true;
      option.textContent = state.files[kind].length ? "No files match the current filter" : "No eligible files found";
      select.appendChild(option);
    } else {
      files.forEach(function (entry) {
        const option = document.createElement("option");
        option.value = entry.relative_path;
        option.textContent = entry.relative_path;
        if (entry.relative_path === state.selected[kind]) {
          option.selected = true;
        }
        select.appendChild(option);
      });
      if (!state.selected[kind] && files.length) {
        select.selectedIndex = -1;
      }
    }

    summary.textContent = state.selected[kind] ? state.selected[kind] : "No " + kind + " selected.";
    updateFileCount(kind, state.files[kind].length + " files");
  }

  function renderSelections() {
    els.selectedCSVValue.textContent = state.selected.csv || "Not selected";
    els.selectedSchemaValue.textContent = state.selected.schema || "Not selected";
  }

  function renderRunState() {
    const snapshot = state.snapshot;
    if (!snapshot) {
      els.runIDValue.textContent = state.runId || "Waiting for submission";
      els.runPhaseValue.textContent = "Idle";
      els.runProgressValue.textContent = "Not started";
      els.workspaceValue.textContent = "No workspace yet";
      els.phaseHeading.textContent = state.health.busy ? "Server busy" : "Ready";
      els.phaseDetail.textContent = state.health.busy ? "A run exists, but this page has not attached to its snapshot yet." : "No active run.";
      els.progressFill.style.width = "0%";
      setBadge(els.runStateBadge, state.health.busy ? "Busy" : "No run selected", state.health.busy ? "warn" : "muted");
      return;
    }

    const latestEvent = snapshot.latest_event || newestEvent();
    const progressPercent = getProgressPercent(snapshot, latestEvent);
    const phaseText = latestEvent ? formatPhase(latestEvent.phase) : formatState(snapshot.state);
    const detail = latestEvent && latestEvent.message ? latestEvent.message : describeState(snapshot.state);

    els.runIDValue.textContent = snapshot.run_id;
    els.runPhaseValue.textContent = phaseText;
    els.runProgressValue.textContent = progressPercent == null ? describeState(snapshot.state) : progressPercent + "%";
    els.workspaceValue.textContent = snapshot.workspace && snapshot.workspace.root_dir ? snapshot.workspace.root_dir : "Workspace unavailable";
    els.phaseHeading.textContent = phaseText;
    els.phaseDetail.textContent = detail;
    els.progressFill.style.width = (progressPercent == null ? 0 : progressPercent) + "%";
    setBadge(els.runStateBadge, formatState(snapshot.state), toneForState(snapshot.state));
  }

  function renderSummary() {
    const snapshot = state.snapshot;
    const workspace = snapshot && snapshot.workspace ? snapshot.workspace : null;
    const result = state.result && state.result.result ? state.result.result : null;
    const cards = [];
    const splitSummary = field(result, "split_summary") || {};
    const validation = field(result, "validation") || {};
    const validationSummary = field(validation, "summary") || {};
    const batchSummary = field(result, "batch_summary") || {};

    cards.push(cardHTML("Inputs", [
      rowHTML("CSV", valueText((workspace && workspace.input_csv_path) || state.selected.csv)),
      rowHTML("Schema", valueText((workspace && workspace.schema_path) || state.selected.schema)),
      rowHTML("Working root", valueText(state.health.working_root)),
    ]));

    cards.push(cardHTML("Workspace", [
      rowHTML("Root", valueText(workspace && workspace.root_dir)),
      rowHTML("Metadata", valueText(workspace && workspace.metadata_path)),
      rowHTML("Success", valueText(workspace && workspace.success_dir)),
      rowHTML("Errors", valueText(workspace && workspace.error_dir)),
      rowHTML("Batch export", valueText(workspace && workspace.batch_export_dir)),
    ]));

    if (result) {
      cards.push(cardHTML("Split", [
        rowHTML("Primary key", valueText(result.split_primary_key)),
        rowHTML("Reused cache", result.split_reused ? "Yes" : "No"),
        rowHTML("Rows", numberText(field(splitSummary, "total_rows", "TotalRows"))),
        rowHTML("Missing keys", numberText(field(splitSummary, "missing_key_rows", "MissingKeyRows"))),
        rowHTML("Output files", numberText(field(splitSummary, "output_files", "OutputFiles"))),
      ]));

      cards.push(cardHTML("Validation", [
        rowHTML("Input files", numberText(field(validation, "file_count", "FileCount"))),
        rowHTML("Failed files", numberText(field(validationSummary, "failed_files", "FailedFiles"))),
        rowHTML("Total rows", numberText(field(validationSummary, "total_rows", "TotalRows"))),
        rowHTML("Valid rows", numberText(field(validationSummary, "valid_rows", "ValidRows"))),
        rowHTML("Invalid rows", numberText(field(validationSummary, "invalid_rows", "InvalidRows"))),
      ]));

      cards.push(cardHTML("Batch export", [
        rowHTML("Input files", numberText(field(batchSummary, "input_files", "InputFiles"))),
        rowHTML("Batches", numberText(field(batchSummary, "batches", "Batches"))),
        rowHTML("Rows written", numberText(field(batchSummary, "total_rows", "TotalRows"))),
        rowHTML("Output dir", valueText(field(batchSummary, "output_dir", "OutputDir"))),
      ]));
    }

    if (snapshot && snapshot.state === "failed") {
      cards.push(cardHTML("Failure", [
        rowHTML("Final error", valueText((state.result && state.result.final_error) || snapshot.final_error)),
      ]));
    }

    if (snapshot && snapshot.state === "completed" && !result) {
      cards.push(cardHTML("Result", [
        rowHTML("Status", "Completed"),
        rowHTML("Details", "Fetching final result…"),
      ]));
    }

    els.summaryCards.innerHTML = cards.join("");
  }

  function renderEvents() {
    if (!state.events.length) {
      els.eventLog.innerHTML = '<li class="event-log__empty">Progress events will appear here once a validation starts.</li>';
      return;
    }

    const html = state.events.slice().reverse().map(function (event) {
      const metaParts = [formatTimestamp(event.time), event.type ? event.type : ""].filter(Boolean);
      if (event.percent != null) {
        metaParts.push(Math.round(Number(event.percent)) + "%");
      }
      return [
        '<li class="event-item">',
        '<div class="event-head">',
        '<span class="event-phase">' + escapeHTML(formatPhase(event.phase)) + "</span>",
        '<span class="badge badge--' + toneForEvent(event.type) + '">' + escapeHTML(event.type || "update") + "</span>",
        "</div>",
        '<p class="event-message">' + escapeHTML(event.message || defaultEventMessage(event)) + "</p>",
        '<p class="event-meta">' + escapeHTML(metaParts.join(" · ")) + "</p>",
        "</li>",
      ].join("");
    });
    els.eventLog.innerHTML = html.join("");
  }

  function updateSubmitState() {
    const busy = Boolean(state.health && state.health.busy);
    const runningThisPage = state.snapshot && state.snapshot.state === "running";
    els.submitButton.disabled = !state.selected.csv || !state.selected.schema || (busy && !runningThisPage);
  }

  function filteredFiles(kind) {
    const filter = state.filters[kind];
    if (!filter) {
      return state.files[kind];
    }
    return state.files[kind].filter(function (entry) {
      return entry.relative_path.toLowerCase().indexOf(filter) >= 0;
    });
  }

  function hasRelativePath(kind, relativePath) {
    if (!relativePath) {
      return false;
    }
    return state.files[kind].some(function (entry) {
      return entry.relative_path === relativePath;
    });
  }

  function updateFileCount(kind, text) {
    const node = kind === "csv" ? els.csvCount : els.schemaCount;
    node.textContent = text;
  }

  function toRelativePath(value) {
    const workingRoot = (state.health && state.health.working_root ? state.health.working_root : "").replace(/\\/g, "/");
    if (!value) {
      return "";
    }
    const normalized = String(value).replace(/\\/g, "/");
    if (workingRoot && normalized.indexOf(workingRoot + "/") === 0) {
      return normalized.slice(workingRoot.length + 1);
    }
    return normalized;
  }

  function setBadge(node, text, tone) {
    node.textContent = text;
    node.className = "badge badge--" + tone;
  }

  function setFormMessage(message, tone) {
    els.formMessage.textContent = message;
    els.formMessage.dataset.tone = tone || "info";
  }

  function clearFormMessage() {
    els.formMessage.textContent = "";
    delete els.formMessage.dataset.tone;
  }

  function newestEvent() {
    return state.events.length ? state.events[state.events.length - 1] : null;
  }

  function getProgressPercent(snapshot, latestEvent) {
    if (latestEvent && latestEvent.percent != null && !Number.isNaN(Number(latestEvent.percent))) {
      return Math.max(0, Math.min(100, Math.round(Number(latestEvent.percent))));
    }
    if (!snapshot) {
      return null;
    }
    if (snapshot.state === "completed") {
      return 100;
    }
    return null;
  }

  function formatPhase(phase) {
    switch (phase) {
      case "run":
        return "Run orchestration";
      case "split":
        return "Split phase";
      case "validate":
        return "Validation phase";
      case "batch":
        return "Batch export";
      default:
        return phase ? phase : "Idle";
    }
  }

  function formatState(runState) {
    switch (runState) {
      case "running":
        return "Running";
      case "completed":
        return "Completed";
      case "failed":
        return "Failed";
      case "queued":
        return "Queued";
      default:
        return "Idle";
    }
  }

  function describeState(runState) {
    switch (runState) {
      case "running":
        return "Run is in progress.";
      case "completed":
        return "Run completed successfully.";
      case "failed":
        return "Run failed. The snapshot and final result remain inspectable.";
      case "queued":
        return "Run has been created and is waiting to start.";
      default:
        return "No active run.";
    }
  }

  function toneForState(runState) {
    switch (runState) {
      case "completed":
        return "ok";
      case "failed":
        return "error";
      case "running":
      case "queued":
        return "info";
      default:
        return "muted";
    }
  }

  function toneForEvent(eventType) {
    switch (eventType) {
      case "completed":
        return "ok";
      case "failed":
        return "error";
      case "started":
      case "progress":
        return "info";
      default:
        return "muted";
    }
  }

  function defaultEventMessage(event) {
    if (event.percent != null) {
      return "Progress update";
    }
    return "Event received";
  }

  function numberText(value) {
    if (value == null || value === "") {
      return "0";
    }
    return String(value);
  }

  function valueText(value) {
    if (value == null || value === "") {
      return "Not available";
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
    return "<div><dt>" + escapeHTML(label) + "</dt><dd>" + escapeHTML(value) + "</dd></div>";
  }

  function formatTimestamp(value) {
    if (!value) {
      return "";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return value;
    }
    return date.toLocaleString();
  }

  function isTerminalState(runState) {
    return runState === "completed" || runState === "failed";
  }

  async function parseJSON(response) {
    try {
      return await response.json();
    } catch (error) {
      return null;
    }
  }

  function stableStringify(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return JSON.stringify(value);
    }
    const keys = Object.keys(value).sort();
    const sorted = {};
    keys.forEach(function (key) {
      sorted[key] = value[key];
    });
    return JSON.stringify(sorted);
  }

  function field(object) {
    if (!object) {
      return undefined;
    }
    for (let index = 1; index < arguments.length; index += 1) {
      const key = arguments[index];
      if (Object.prototype.hasOwnProperty.call(object, key) && object[key] != null) {
        return object[key];
      }
    }
    return undefined;
  }

  function escapeHTML(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  init();
})();
