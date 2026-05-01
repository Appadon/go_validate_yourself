(function () {
  "use strict";

  const bootstrap = window.GVY_UI_BOOTSTRAP || {};
  const phaseOrder = ["split", "validate", "batch"];
  const pickerProfiles = {
    csv: { apiKind: "csv", mode: "file", title: "Select main CSV", subtitle: "Browse the working directory and choose one CSV file.", target: "selectedCsv" },
    schema: { apiKind: "schema", mode: "file", title: "Select schema JSON", subtitle: "Browse the working directory and choose one schema JSON file.", target: "selectedSchema" },
    validateCsv: { apiKind: "csv", mode: "file", title: "Select validation CSV", subtitle: "Choose one existing split CSV file.", target: "validateCsvInput" },
    validateDir: { apiKind: "csv", mode: "dir", title: "Select validation directory", subtitle: "Choose a directory that contains existing split CSV output.", target: "validateDirInput" },
    batchDir: { apiKind: "csv", mode: "dir", title: "Select batch input directory", subtitle: "Choose a directory that contains existing parquet output.", target: "batchInputDirInput" },
  };

  const state = {
    health: bootstrap.server || { status: "ok", busy: false, version: "v1", working_root: "" },
    runId: bootstrap.latest_run_id || "",
    snapshot: null,
    result: null,
    configDefaults: null,
    configDefaultsLoaded: false,
    preview: {
      status: "idle",
      resolved: null,
      error: "",
      localError: "",
    },
    lastSubmittedConfig: null,
    lastSubmittedResolved: null,
    browser: {
      activeKind: "",
      csv: {
        currentPath: "",
        parentPath: "",
        entries: [],
      },
      schema: {
        currentPath: "",
        parentPath: "",
        entries: [],
      },
    },
    filters: {
      csv: "",
      schema: "",
      validateCsv: "",
      validateDir: "",
      batchDir: "",
    },
    pickerSelection: {
      csv: "",
      schema: "",
      validateCsv: "",
      validateDir: "",
      batchDir: "",
    },
    selected: {
      csv: "",
      schema: "",
    },
    events: [],
    seenEvents: new Set(),
    stream: null,
    pendingRefresh: 0,
    pendingConfigRun: false,
    pendingAttach: 0,
    attachingRunId: "",
    resolveTimer: 0,
    resolveSequence: 0,
    wizard: {
      activeStep: 0,
      maxStep: 4,
    },
  };

  const els = {
    form: document.getElementById("run-form"),
    wizardCards: document.querySelectorAll(".wizard-card[data-step]"),
    wizardStepButtons: document.querySelectorAll("[data-wizard-step]"),
    wizardBackButton: document.getElementById("wizard-back-button"),
    wizardNextButton: document.getElementById("wizard-next-button"),
    wizardStepStatus: document.getElementById("wizard-step-status"),
    refreshFilesButton: document.getElementById("refresh-files-button"),
    defaultsStatus: document.getElementById("defaults-status"),
    phaseSplit: document.getElementById("phase-split"),
    phaseValidate: document.getElementById("phase-validate"),
    phaseBatch: document.getElementById("phase-batch"),
    mainCSVGroup: document.getElementById("main-csv-group"),
    schemaGroup: document.getElementById("schema-group"),
    sourceInputsSection: document.getElementById("source-inputs-section"),
    validateCSVField: document.getElementById("validate-csv-field"),
    validateDirField: document.getElementById("validate-dir-field"),
    batchInputDirField: document.getElementById("batch-input-dir-field"),
    validateCSVInput: document.getElementById("validate-csv-input"),
    validateDirInput: document.getElementById("validate-dir-input"),
    batchInputDirInput: document.getElementById("batch-input-dir-input"),
    validateCSVOpenButton: document.getElementById("validate-csv-open-button"),
    validateDirOpenButton: document.getElementById("validate-dir-open-button"),
    batchInputDirOpenButton: document.getElementById("batch-input-dir-open-button"),
    workersInput: document.getElementById("workers-input"),
    splitPrimaryKeyInput: document.getElementById("split-primary-key-input"),
    splitOutputDirInput: document.getElementById("split-output-dir-input"),
    successDirInput: document.getElementById("success-dir-input"),
    errorDirInput: document.getElementById("error-dir-input"),
    batchExportDirInput: document.getElementById("batch-export-dir-input"),
    batchSizeInput: document.getElementById("batch-size-input"),
    resumePolicySelect: document.getElementById("resume-policy-select"),
    writeEmptyErrorInput: document.getElementById("write-empty-error-input"),
    clearOutputsInput: document.getElementById("clear-outputs-input"),
    previewStatus: document.getElementById("preview-status"),
    previewErrors: document.getElementById("preview-errors"),
    resolvedPreview: document.getElementById("resolved-preview"),
    csvOpenButton: document.getElementById("csv-open-button"),
    schemaOpenButton: document.getElementById("schema-open-button"),
    csvCount: document.getElementById("csv-count"),
    schemaCount: document.getElementById("schema-count"),
    csvSelectionSummary: document.getElementById("csv-selection-summary"),
    schemaSelectionSummary: document.getElementById("schema-selection-summary"),
    submitButton: document.getElementById("submit-button"),
    refreshButton: document.getElementById("refresh-button"),
    formMessage: document.getElementById("form-message"),
    serverStatusText: document.getElementById("server-status-text"),
    serverStatusBadge: document.getElementById("server-status-badge"),
    runStateBadge: document.getElementById("run-state-badge"),
    runIDValue: document.getElementById("run-id-value"),
    runStageValue: document.getElementById("run-stage-value"),
    runProgressValue: document.getElementById("run-progress-value"),
    stageDetail: document.getElementById("stage-detail"),
    phaseHeading: document.getElementById("phase-heading"),
    phaseDetail: document.getElementById("phase-detail"),
    progressFill: document.getElementById("progress-fill"),
    summaryCards: document.getElementById("summary-cards"),
    eventLog: document.getElementById("event-log"),
    pickerModal: document.getElementById("picker-modal"),
    pickerBackdrop: document.getElementById("picker-backdrop"),
    pickerTitle: document.getElementById("picker-title"),
    pickerSubtitle: document.getElementById("picker-subtitle"),
    pickerCloseButton: document.getElementById("picker-close-button"),
    pickerFilterInput: document.getElementById("picker-filter-input"),
    pickerPathValue: document.getElementById("picker-path-value"),
    pickerUpButton: document.getElementById("picker-up-button"),
    pickerDirectories: document.getElementById("picker-directories"),
    pickerSelect: document.getElementById("picker-select"),
    pickerSelectionSummary: document.getElementById("picker-selection-summary"),
    pickerCurrentDirButton: document.getElementById("picker-current-dir-button"),
    pickerChooseButton: document.getElementById("picker-choose-button"),
  };

  function init() {
    bindEvents();
    render();
    loadConfigDefaults();
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

    els.wizardBackButton.addEventListener("click", previousWizardStep);
    els.wizardNextButton.addEventListener("click", nextWizardStep);
    els.wizardStepButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        setWizardStep(integerValue(button.getAttribute("data-wizard-step")));
      });
    });

    els.csvOpenButton.addEventListener("click", function () {
      openPicker("csv");
    });

    els.schemaOpenButton.addEventListener("click", function () {
      openPicker("schema");
    });

    els.validateCSVOpenButton.addEventListener("click", function () {
      openPicker("validateCsv");
    });

    els.validateDirOpenButton.addEventListener("click", function () {
      openPicker("validateDir");
    });

    els.batchInputDirOpenButton.addEventListener("click", function () {
      openPicker("batchDir");
    });

    configControlElements().forEach(function (control) {
      control.addEventListener("input", handleConfigControlChange);
      control.addEventListener("change", handleConfigControlChange);
    });

    els.pickerFilterInput.addEventListener("input", function () {
      const kind = state.browser.activeKind;
      if (!kind) {
        return;
      }
      state.filters[kind] = els.pickerFilterInput.value.trim().toLowerCase();
      renderPicker();
    });

    els.pickerSelect.addEventListener("change", function () {
      const kind = state.browser.activeKind;
      if (!kind) {
        return;
      }
      state.pickerSelection[kind] = els.pickerSelect.value || "";
      updatePickerSelectionState();
    });

    els.pickerSelect.addEventListener("dblclick", function () {
      commitPickerSelection();
    });

    els.pickerCurrentDirButton.addEventListener("click", function () {
      commitCurrentDirectorySelection();
    });

    els.pickerUpButton.addEventListener("click", function () {
      const kind = state.browser.activeKind;
      if (kind) {
        browseUp(kind);
      }
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

    els.pickerChooseButton.addEventListener("click", function () {
      commitPickerSelection();
    });

    els.pickerCloseButton.addEventListener("click", closePicker);
    els.pickerBackdrop.addEventListener("click", closePicker);
  }

  function configControlElements() {
    return [
      els.phaseSplit,
      els.phaseValidate,
      els.phaseBatch,
      els.validateCSVInput,
      els.validateDirInput,
      els.batchInputDirInput,
      els.workersInput,
      els.splitPrimaryKeyInput,
      els.splitOutputDirInput,
      els.successDirInput,
      els.errorDirInput,
      els.batchExportDirInput,
      els.batchSizeInput,
      els.resumePolicySelect,
      els.writeEmptyErrorInput,
      els.clearOutputsInput,
    ];
  }

  function handleConfigControlChange() {
    renderConfigVisibility();
    clearFormMessage();
    scheduleResolvePreview();
    render();
  }

  /*
  loadConfigDefaults hydrates the form from GET /api/config/defaults so the
  browser starts from the same canonical values the backend will resolve.
  */
  async function loadConfigDefaults() {
    try {
      const response = await fetch("/api/config/defaults");
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load backend config defaults");
      }
      state.configDefaults = payload.defaults || {};
      state.configDefaultsLoaded = true;
      applyDefaultsToForm(state.configDefaults);
      setBadge(els.defaultsStatus, "Defaults loaded", "ok");
      render();
      scheduleResolvePreview(0);
    } catch (error) {
      state.configDefaultsLoaded = false;
      setBadge(els.defaultsStatus, "Defaults unavailable", "error");
      state.preview.status = "error";
      state.preview.error = error.message || "Could not load backend config defaults";
      renderPreview();
      updateSubmitState();
    }
  }

  /*
  applyDefaultsToForm copies backend-supplied defaults into editable controls
  without hard-coding fallback values in JavaScript.
  */
  function applyDefaultsToForm(defaults) {
    const defaultPhases = defaultPhasesForConfig(defaults);
    els.phaseSplit.checked = defaultPhases.indexOf("split") >= 0;
    els.phaseValidate.checked = defaultPhases.indexOf("validate") >= 0;
    els.phaseBatch.checked = defaultPhases.indexOf("batch") >= 0;
    els.workersInput.value = valueOrEmpty(defaults.runtime && defaults.runtime.workers);
    els.splitPrimaryKeyInput.value = valueOrEmpty(defaults.split && defaults.split.primary_key);
    els.splitOutputDirInput.value = valueOrEmpty(defaults.outputs && defaults.outputs.split_dir);
    els.successDirInput.value = valueOrEmpty(defaults.outputs && defaults.outputs.success_dir);
    els.errorDirInput.value = valueOrEmpty(defaults.outputs && defaults.outputs.error_dir);
    els.batchExportDirInput.value = valueOrEmpty(defaults.outputs && defaults.outputs.batch_export_dir);
    els.batchSizeInput.value = valueOrEmpty(defaults.batch && defaults.batch.size);
    els.resumePolicySelect.value = (defaults.pipeline && defaults.pipeline.resume_policy) || els.resumePolicySelect.value;
    els.writeEmptyErrorInput.checked = Boolean(defaults.validation && defaults.validation.write_empty_error);
    els.clearOutputsInput.checked = Boolean((defaults.validation && defaults.validation.clear_outputs) || (defaults.batch && defaults.batch.clear_output));
    renderConfigVisibility();
  }

  function defaultPhasesForConfig(defaults) {
    const explicit = defaults && defaults.pipeline && Array.isArray(defaults.pipeline.phases) ? defaults.pipeline.phases : [];
    if (explicit.length) {
      return explicit;
    }
    switch ((defaults && defaults.mode) || "") {
      case "split":
        return ["split"];
      case "validate":
        return ["validate"];
      case "batch":
        return ["batch"];
      case "server":
        return [];
      default:
        return ["split", "validate", "batch"];
    }
  }

  /*
  buildCurrentConfig overlays the current UI state onto the backend defaults
  and returns the exact config object sent to resolve and run endpoints.
  */
  function buildCurrentConfig() {
    const cfg = deepClone(state.configDefaults || {});
    ensureConfigShape(cfg);
    const phases = selectedPhases();
    cfg.mode = "auto";
    cfg.pipeline.phases = phases;
    cfg.pipeline.resume_policy = els.resumePolicySelect.value;
    cfg.inputs.main_csv = phases.indexOf("split") >= 0 ? state.selected.csv : "";
    cfg.inputs.schema = phases.indexOf("validate") >= 0 ? state.selected.schema : "";
    cfg.inputs.validate_csv = phases.indexOf("validate") >= 0 && phases.indexOf("split") < 0 ? els.validateCSVInput.value.trim() : "";
    cfg.inputs.validate_dir = phases.indexOf("validate") >= 0 && phases.indexOf("split") < 0 ? els.validateDirInput.value.trim() : "";
    cfg.outputs.split_dir = els.splitOutputDirInput.value.trim();
    cfg.outputs.success_dir = els.successDirInput.value.trim();
    cfg.outputs.error_dir = els.errorDirInput.value.trim();
    cfg.outputs.batch_export_dir = els.batchExportDirInput.value.trim();
    cfg.split.primary_key = els.splitPrimaryKeyInput.value.trim();
    cfg.validation.write_empty_error = els.writeEmptyErrorInput.checked;
    cfg.validation.clear_outputs = els.clearOutputsInput.checked;
    cfg.batch.input_dir = phases.indexOf("batch") >= 0 && phases.indexOf("validate") < 0 ? els.batchInputDirInput.value.trim() : "";
    cfg.batch.size = integerValue(els.batchSizeInput.value);
    cfg.batch.clear_output = els.clearOutputsInput.checked;
    cfg.runtime.workers = integerValue(els.workersInput.value);
    return cfg;
  }

  function ensureConfigShape(cfg) {
    cfg.pipeline = cfg.pipeline || {};
    cfg.inputs = cfg.inputs || {};
    cfg.outputs = cfg.outputs || {};
    cfg.split = cfg.split || {};
    cfg.validation = cfg.validation || {};
    cfg.batch = cfg.batch || {};
    cfg.runtime = cfg.runtime || {};
    cfg.server = cfg.server || {};
  }

  function selectedPhases() {
    return phaseOrder.filter(function (phase) {
      if (phase === "split") {
        return els.phaseSplit.checked;
      }
      if (phase === "validate") {
        return els.phaseValidate.checked;
      }
      return els.phaseBatch.checked;
    });
  }

  /*
  scheduleResolvePreview debounces calls to POST /api/config/resolve so every
  meaningful form change is validated by the backend before submission.
  */
  function scheduleResolvePreview(delayMs) {
    window.clearTimeout(state.resolveTimer);
    state.resolveTimer = window.setTimeout(function () {
      resolvePreviewNow();
    }, delayMs == null ? 250 : delayMs);
  }

  /*
  resolvePreviewNow posts the current config to the server resolver and stores
  either the effective config or the resolver's validation error for rendering.
  */
  async function resolvePreviewNow() {
    window.clearTimeout(state.resolveTimer);
    const localError = localConfigError();
    if (localError) {
      state.preview = { status: "error", resolved: null, error: localError, localError: localError };
      renderPreview();
      updateSubmitState();
      return false;
    }
    if (!state.configDefaultsLoaded) {
      state.preview = { status: "error", resolved: null, error: "Backend defaults have not loaded yet.", localError: "" };
      renderPreview();
      updateSubmitState();
      return false;
    }

    const sequence = state.resolveSequence + 1;
    state.resolveSequence = sequence;
    state.preview.status = "pending";
    state.preview.error = "";
    renderPreview();
    updateSubmitState();

    try {
      const response = await fetch("/api/config/resolve", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(buildCurrentConfig()),
      });
      const payload = await parseJSON(response);
      if (sequence !== state.resolveSequence) {
        return false;
      }
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Config resolution failed");
      }
      state.preview.status = "ok";
      state.preview.resolved = payload.resolved_config || null;
      state.preview.error = "";
      renderPreview();
      updateSubmitState();
      return true;
    } catch (error) {
      if (sequence !== state.resolveSequence) {
        return false;
      }
      state.preview.status = "error";
      state.preview.resolved = null;
      state.preview.error = error.message || "Config resolution failed";
      renderPreview();
      updateSubmitState();
      return false;
    }
  }

  function localConfigError() {
    if (!selectedPhases().length) {
      return "Select at least one pipeline phase.";
    }
    return "";
  }

  async function refreshFileLists() {
    clearFormMessage();
    await Promise.all([loadFileList("csv", state.browser.csv.currentPath), loadFileList("schema", state.browser.schema.currentPath)]);
    render();
  }

  async function loadFileList(kind, path) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    const apiKind = profile.apiKind;
    updateFileCount(apiKind, "Loading…");
    try {
      const params = new URLSearchParams();
      params.set("kind", apiKind);
      if (path) {
        params.set("path", path);
      }
      const response = await fetch("/api/files?" + params.toString());
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load " + apiKind + " files");
      }
      state.browser[apiKind].currentPath = payload.current_path || "";
      state.browser[apiKind].parentPath = payload.parent_path || "";
      state.browser[apiKind].entries = payload.entries || [];
      if (pickerProfiles[kind] && pickerProfiles[kind].mode === "file" && !hasRelativePath(kind, state.pickerSelection[kind])) {
        state.pickerSelection[kind] = "";
      }
      if (kind === "schema" && state.selected.schema && !hasRelativePath("schema", state.selected.schema)) {
        state.selected.schema = "";
        scheduleResolvePreview();
      }
      if (kind === "csv" && state.selected.csv && !hasRelativePath("csv", state.selected.csv)) {
        state.selected.csv = "";
        scheduleResolvePreview();
      }
      renderPicker();
      renderSelectionSummary("csv");
      renderSelectionSummary("schema");
    } catch (error) {
      state.browser[apiKind].entries = [];
      renderPicker();
      renderSelectionSummary("csv");
      renderSelectionSummary("schema");
      setFormMessage(error.message || "Could not load file lists", "error");
    }
  }

  /*
  submitRun uses the config-first run endpoint with the same config object
  that was resolved in preview, preserving the simple CSV plus schema path.
  */
  async function submitRun() {
    const previewOK = state.preview.status === "ok" || await resolvePreviewNow();
    if (!previewOK) {
      setFormMessage("Fix the configuration errors before starting a run.", "warn");
      return;
    }

    closeStream();
    els.submitButton.disabled = true;
    const config = buildCurrentConfig();
    state.lastSubmittedConfig = deepClone(config);
    state.lastSubmittedResolved = state.preview.resolved ? deepClone(state.preview.resolved) : null;
    setFormMessage("Creating config-driven run from the current pipeline settings…", "info");
    startPendingConfigRunAttach();

    try {
      const response = await fetch("/api/runs/config", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(config),
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
      state.result = {
        result: {
          resolved_config: payload.resolved_config || state.lastSubmittedResolved,
          result: payload.result || null,
        },
      };
      state.lastSubmittedResolved = payload.resolved_config || state.lastSubmittedResolved;
      state.health.busy = false;
      stopPendingConfigRunAttach();
      replaceSnapshot(payload.run);
      render();
      setFormMessage("Run completed. Final result is shown below.", "ok");
      refreshHealth();
    } catch (error) {
      setFormMessage(error.message || "Run creation failed", "error");
    } finally {
      stopPendingConfigRunAttach();
      render();
    }
  }

  /*
  startPendingConfigRunAttach polls health while POST /api/runs/config is
  still pending, then attaches the UI to the run snapshot and SSE stream.
  */
  function startPendingConfigRunAttach() {
    state.pendingConfigRun = true;
    state.attachingRunId = "";
    window.clearTimeout(state.pendingAttach);
    state.pendingAttach = window.setTimeout(pollPendingConfigRunAttach, 250);
  }

  function stopPendingConfigRunAttach() {
    state.pendingConfigRun = false;
    state.attachingRunId = "";
    window.clearTimeout(state.pendingAttach);
    state.pendingAttach = 0;
  }

  /*
  pollPendingConfigRunAttach discovers the run ID created by the synchronous
  config-run request and keeps the progress panel connected while it runs.
  */
  async function pollPendingConfigRunAttach() {
    if (!state.pendingConfigRun) {
      return;
    }

    try {
      const response = await fetch("/health");
      if (response.ok) {
        state.health = await response.json();
        const latestRunId = state.health.latest_run_id || "";
        if (state.health.busy && latestRunId && latestRunId !== state.attachingRunId) {
          state.attachingRunId = latestRunId;
          await syncRun(latestRunId);
          setFormMessage("Run created. Streaming progress now.", "ok");
        } else if (state.health.busy && latestRunId && state.snapshot && state.snapshot.run_id === latestRunId && !isTerminalState(state.snapshot.state)) {
          openStream(latestRunId);
          render();
        } else {
          render();
        }
      }
    } catch (error) {
      render();
    } finally {
      if (state.pendingConfigRun) {
        state.pendingAttach = window.setTimeout(pollPendingConfigRunAttach, 900);
      }
    }
  }

  async function refreshHealth() {
    try {
      const response = await fetch("/health");
      if (!response.ok) {
        return;
      }
      state.health = await response.json();
      if (state.health.busy && state.health.latest_run_id && (!state.snapshot || state.snapshot.run_id !== state.health.latest_run_id)) {
        syncRun(state.health.latest_run_id);
      }
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
    scheduleResolvePreview();
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
    renderWizard();
    renderServerState();
    renderConfigVisibility();
    renderSelectionSummary("csv");
    renderSelectionSummary("schema");
    renderPreview();
    renderRunState();
    renderSummary();
    renderEvents();
    updateSubmitState();
    renderPicker();
  }

  function setWizardStep(step) {
    const nextStep = clampStep(step);
    if (state.wizard.activeStep === nextStep) {
      renderWizard();
      return;
    }
    state.wizard.activeStep = nextStep;
    renderWizard();
  }

  function nextWizardStep() {
    setWizardStep(state.wizard.activeStep + 1);
  }

  function previousWizardStep() {
    setWizardStep(state.wizard.activeStep - 1);
  }

  function renderWizard() {
    const activeStep = clampStep(state.wizard.activeStep);
    state.wizard.activeStep = activeStep;

    els.wizardCards.forEach(function (card) {
      const step = integerValue(card.getAttribute("data-step"));
      const active = step === activeStep;
      card.classList.toggle("wizard-card--active", active);
      card.setAttribute("aria-hidden", active ? "false" : "true");
    });

    els.wizardStepButtons.forEach(function (button) {
      const step = integerValue(button.getAttribute("data-wizard-step"));
      const active = step === activeStep;
      button.classList.toggle("wizard-step--active", active);
      if (active) {
        button.setAttribute("aria-current", "step");
      } else {
        button.removeAttribute("aria-current");
      }
    });

    els.wizardBackButton.disabled = activeStep === 0;
    els.wizardNextButton.disabled = activeStep === state.wizard.maxStep;
    els.wizardStepStatus.textContent = "Step " + (activeStep + 1) + " of " + (state.wizard.maxStep + 1);
  }

  function clampStep(step) {
    if (!Number.isFinite(step)) {
      return 0;
    }
    return Math.min(Math.max(step, 0), state.wizard.maxStep);
  }

  function renderServerState() {
    const busy = Boolean(state.health && state.health.busy);
    els.serverStatusText.textContent = busy ? "Busy" : "Idle";
    setBadge(els.serverStatusBadge, busy ? "Single run occupied" : "Ready for a new run", busy ? "warn" : "ok");
  }

  function renderConfigVisibility() {
    const phases = selectedPhases();
    const split = phases.indexOf("split") >= 0;
    const validate = phases.indexOf("validate") >= 0;
    const batch = phases.indexOf("batch") >= 0;
    els.mainCSVGroup.hidden = !split;
    els.schemaGroup.hidden = !validate;
    els.validateCSVField.hidden = !validate || split;
    els.validateDirField.hidden = !validate || split;
    els.batchInputDirField.hidden = !batch || validate;
    els.sourceInputsSection.hidden = !((validate && !split) || (batch && !validate));
  }

  function renderSelectionSummary(kind) {
    const summary = kind === "csv" ? els.csvSelectionSummary : els.schemaSelectionSummary;
    summary.textContent = state.selected[kind] ? displayFileName(state.selected[kind]) : "No " + kind + " selected";
  }

  function renderPreview() {
    if (state.preview.status === "pending") {
      setBadge(els.previewStatus, "Resolving", "info");
    } else if (state.preview.status === "ok") {
      setBadge(els.previewStatus, "Ready", "ok");
    } else if (state.preview.status === "error") {
      setBadge(els.previewStatus, "Invalid", "error");
    } else {
      setBadge(els.previewStatus, "Waiting", "muted");
    }

    if (state.preview.error) {
      els.previewErrors.hidden = false;
      els.previewErrors.textContent = state.preview.error;
    } else {
      els.previewErrors.hidden = true;
      els.previewErrors.textContent = "";
    }

    const resolved = state.preview.resolved;
    if (!resolved) {
      els.resolvedPreview.innerHTML = rowHTML("Status", state.preview.error || "Choose inputs to preview the effective pipeline.");
      return;
    }

    const plan = resolved.plan || {};
    const rows = [
      rowHTML("Phases", listText(plan.phases)),
      rowHTML("Resume policy", valueText(plan.resume_policy)),
      rowHTML("Workers", valueText(resolved.effective_workers)),
    ];
    if (phaseInPlan(resolved, "split")) {
      rows.push(rowHTML("Primary key", splitPrimaryKeyText(resolved)));
      rows.push(rowHTML("Split input", valueText(plan.split_input_csv)));
      rows.push(rowHTML("Split output", valueText(plan.split_output_dir)));
    }
    if (phaseInPlan(resolved, "validate")) {
      rows.push(rowHTML("Validate input", valueText(plan.validate_input_csv || plan.validate_input_dir)));
      rows.push(rowHTML("Schema", valueText(plan.validate_schema)));
      rows.push(rowHTML("Success output", valueText(plan.validation_success_dir)));
      rows.push(rowHTML("Error output", valueText(plan.validation_error_dir)));
    }
    if (phaseInPlan(resolved, "batch")) {
      rows.push(rowHTML("Batch input", valueText(plan.batch_input_dir)));
      rows.push(rowHTML("Batch output", valueText(plan.batch_output_dir)));
    }
    els.resolvedPreview.innerHTML = rows.join("");
  }

  function renderPicker() {
    const kind = state.browser.activeKind;
    els.pickerModal.hidden = !kind;
    if (!kind) {
      return;
    }

    const profile = pickerProfiles[kind];
    const apiKind = profile.apiKind;
    const select = els.pickerSelect;
    const files = profile.mode === "file" ? filteredFiles(kind) : filteredDirectories(kind);
    const directories = filteredDirectories(kind);
    const currentPath = state.browser[apiKind].currentPath || "";
    const selectedValue = state.pickerSelection[kind] || "";

    els.pickerTitle.textContent = profile.title;
    els.pickerSubtitle.textContent = profile.subtitle;
    els.pickerFilterInput.value = state.filters[kind] || "";
    els.pickerPathValue.textContent = "/" + currentPath;
    els.pickerUpButton.disabled = !state.browser[apiKind].parentPath && !currentPath;
    els.pickerCurrentDirButton.hidden = profile.mode !== "dir";
    els.pickerChooseButton.textContent = profile.mode === "dir" ? "Use selected directory" : "Use selected file";

    if (!directories.length) {
      els.pickerDirectories.innerHTML = '<span class="directory-empty">No subdirectories here.</span>';
    } else {
      els.pickerDirectories.innerHTML = directories.map(function (entry) {
        return '<button class="directory-chip" type="button" data-kind="' + kind + '" data-path="' + escapeHTML(entry.relative_path) + '">/' + escapeHTML(entry.name) + '</button>';
      }).join("");
      els.pickerDirectories.querySelectorAll("[data-path]").forEach(function (button) {
        button.addEventListener("click", function () {
          loadFileList(kind, button.getAttribute("data-path"));
        });
      });
    }

    select.innerHTML = "";
    if (!files.length) {
      const option = document.createElement("option");
      option.disabled = true;
      option.textContent = profile.mode === "dir" ? "No subdirectories match the current filter" : hasFileEntries(kind) ? "No files match the current filter" : "No matching files in this directory";
      select.appendChild(option);
    } else {
      files.forEach(function (entry) {
        const option = document.createElement("option");
        option.value = entry.relative_path;
        option.textContent = entry.name;
        if (entry.relative_path === selectedValue) {
          option.selected = true;
        }
        select.appendChild(option);
      });
      if (!selectedValue && files.length) {
        select.selectedIndex = -1;
      }
    }

    updateFileCount(apiKind, directories.length + " dirs · " + selectableFileEntries(kind).length + " files");
    updatePickerSelectionState();
  }

  function renderRunState() {
    const snapshot = state.snapshot;
    if (!snapshot) {
      els.runIDValue.textContent = state.runId || "Waiting for submission";
      els.runStageValue.textContent = state.health.busy ? "Attached elsewhere" : "Idle";
      els.runProgressValue.textContent = "Not started";
      els.stageDetail.textContent = state.pendingConfigRun ? "Attaching" : state.health.busy ? "Run in progress" : "Preparing";
      els.phaseHeading.textContent = state.pendingConfigRun ? "Starting run" : state.health.busy ? "Server busy" : "Ready";
      els.phaseDetail.textContent = state.pendingConfigRun ? "Waiting for the run snapshot and progress stream." : state.health.busy ? "A run exists, but this page has not attached to its snapshot yet." : "No active run.";
      els.progressFill.style.width = "0%";
      setBadge(els.runStateBadge, state.health.busy ? "Busy" : "No run selected", state.health.busy ? "warn" : "muted");
      return;
    }

    const latestEvent = snapshot.latest_event || newestEvent();
    const progressPercent = getProgressPercent(snapshot, latestEvent);
    const stageInfo = getStageInfo(snapshot, latestEvent);
    const phaseText = stageInfo.phase ? formatPhase(stageInfo.phase) : latestEvent ? formatPhase(latestEvent.phase) : formatState(snapshot.state);
    const detail = latestEvent && latestEvent.message ? latestEvent.message : describeState(snapshot.state);

    els.runIDValue.textContent = snapshot.run_id;
    els.runStageValue.textContent = stageInfo.label;
    els.runProgressValue.textContent = progressPercent == null ? describeState(snapshot.state) : progressPercent + "%";
    els.stageDetail.textContent = stageInfo.label;
    els.phaseHeading.textContent = phaseText;
    els.phaseDetail.textContent = detail;
    els.progressFill.style.width = (progressPercent == null ? 0 : progressPercent) + "%";
    setBadge(els.runStateBadge, formatState(snapshot.state), toneForState(snapshot.state));
  }

  function renderSummary() {
    const snapshot = state.snapshot;
    const workspace = snapshot && snapshot.workspace ? snapshot.workspace : null;
    const final = finalResultInfo();
    const resolved = final.resolved || state.lastSubmittedResolved || null;
    const result = final.pipeline || null;
    const cards = [];
    const splitSummary = field(result, "split_summary") || {};
    const validation = field(result, "validation", "validation_dir") || {};
    const validationSummary = field(validation, "summary") || {};
    const batchSummary = field(result, "batch_summary") || {};

    if (resolved) {
      const plan = resolved.plan || {};
      cards.push(cardHTML("Effective config", [
        rowHTML("Phases", listText(plan.phases)),
        rowHTML("Workers", valueText(resolved.effective_workers)),
        rowHTML("Primary key", splitPrimaryKeyText(resolved)),
        rowHTML("Resume", valueText(plan.resume_policy)),
      ]));

      cards.push(cardHTML("Effective inputs", [
        rowHTML("Split CSV", valueText(plan.split_input_csv)),
        rowHTML("Validate input", valueText(plan.validate_input_csv || plan.validate_input_dir)),
        rowHTML("Schema", valueText(plan.validate_schema)),
        rowHTML("Batch input", valueText(plan.batch_input_dir)),
      ]));

      cards.push(cardHTML("Effective outputs", [
        rowHTML("Split", valueText(plan.split_output_dir)),
        rowHTML("Success", valueText(plan.validation_success_dir)),
        rowHTML("Errors", valueText(plan.validation_error_dir)),
        rowHTML("Batch export", valueText(plan.batch_output_dir)),
      ]));
    } else {
      cards.push(cardHTML("Inputs", [
        rowHTML("CSV", valueText((workspace && workspace.input_csv_path) || state.selected.csv)),
        rowHTML("Schema", valueText((workspace && workspace.schema_path) || state.selected.schema)),
      ]));

      cards.push(cardHTML("Outputs", [
        rowHTML("Success", valueText(workspace && workspace.success_dir)),
        rowHTML("Errors", valueText(workspace && workspace.error_dir)),
        rowHTML("Batch export", valueText(workspace && workspace.batch_export_dir)),
      ]));
    }

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

    if (!result && snapshot && snapshot.state === "running") {
      cards.push(cardHTML("Outputs", [
        rowHTML("Success", valueText(workspace && workspace.success_dir)),
        rowHTML("Errors", valueText(workspace && workspace.error_dir)),
        rowHTML("Batch export", valueText(workspace && workspace.batch_export_dir)),
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
    const previewOK = state.preview.status === "ok";
    els.submitButton.disabled = !previewOK || (busy && !runningThisPage);
  }

  function filteredFiles(kind) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    const filter = state.filters[kind];
    const files = selectableFileEntries(kind);
    if (!filter) {
      return files;
    }
    return files.filter(function (entry) {
      return entry.relative_path.toLowerCase().indexOf(filter) >= 0;
    });
  }

  function filteredDirectories(kind) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    const filter = state.filters[kind];
    const directories = directoryEntries(profile.apiKind);
    if (!filter) {
      return directories;
    }
    return directories.filter(function (entry) {
      return entry.relative_path.toLowerCase().indexOf(filter) >= 0;
    });
  }

  function fileEntries(kind) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    return (state.browser[profile.apiKind].entries || []).filter(function (entry) {
      return !entry.is_dir;
    });
  }

  function selectableFileEntries(kind) {
    return fileEntries(kind).filter(function (entry) {
      return !(kind === "schema" && entry.relative_path === "schema.example.json");
    });
  }

  function directoryEntries(kind) {
    const apiKind = pickerProfiles[kind] ? pickerProfiles[kind].apiKind : kind;
    return (state.browser[apiKind].entries || []).filter(function (entry) {
      return entry.is_dir;
    });
  }

  function hasFileEntries(kind) {
    return selectableFileEntries(kind).length > 0;
  }

  function hasRelativePath(kind, relativePath) {
    if (!relativePath) {
      return false;
    }
    return selectableFileEntries(kind).some(function (entry) {
      return entry.relative_path === relativePath;
    });
  }

  function browseUp(kind) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    const targetPath = state.browser[profile.apiKind].parentPath || "";
    loadFileList(kind, targetPath);
  }

  function openPicker(kind) {
    const profile = pickerProfiles[kind];
    state.browser.activeKind = kind;
    state.pickerSelection[kind] = pickerCurrentTargetValue(profile) || "";
    if (!state.browser[profile.apiKind].entries.length) {
      loadFileList(kind, state.browser[profile.apiKind].currentPath);
    }
    renderPicker();
  }

  function closePicker() {
    state.browser.activeKind = "";
    renderPicker();
  }

  function currentPickerValue() {
    const kind = state.browser.activeKind;
    return kind ? state.pickerSelection[kind] || "" : "";
  }

  function commitCurrentDirectorySelection() {
    const kind = state.browser.activeKind;
    const profile = pickerProfiles[kind];
    if (!kind || !profile || profile.mode !== "dir") {
      return;
    }
    applyPickerValue(profile, state.browser[profile.apiKind].currentPath || ".");
    closePicker();
    render();
    scheduleResolvePreview();
  }

  function commitPickerSelection() {
    const kind = state.browser.activeKind;
    const profile = pickerProfiles[kind];
    const value = currentPickerValue();
    if (!kind || !profile || !value) {
      return;
    }
    applyPickerValue(profile, value);
    clearFormMessage();
    closePicker();
    render();
    scheduleResolvePreview();
  }

  function pickerCurrentTargetValue(profile) {
    switch (profile.target) {
      case "selectedCsv":
        return state.selected.csv;
      case "selectedSchema":
        return state.selected.schema;
      case "validateCsvInput":
        return els.validateCSVInput.value.trim();
      case "validateDirInput":
        return els.validateDirInput.value.trim();
      case "batchInputDirInput":
        return els.batchInputDirInput.value.trim();
      default:
        return "";
    }
  }

  function applyPickerValue(profile, value) {
    switch (profile.target) {
      case "selectedCsv":
        state.selected.csv = value;
        break;
      case "selectedSchema":
        state.selected.schema = value;
        break;
      case "validateCsvInput":
        els.validateCSVInput.value = value;
        break;
      case "validateDirInput":
        els.validateDirInput.value = value;
        break;
      case "batchInputDirInput":
        els.batchInputDirInput.value = value;
        break;
      default:
        break;
    }
  }

  function updatePickerSelectionState() {
    const kind = state.browser.activeKind;
    const profile = pickerProfiles[kind];
    const value = kind ? state.pickerSelection[kind] || "" : "";
    els.pickerSelectionSummary.textContent = value ? value : (profile && profile.mode === "dir" ? "No directory selected." : "No file selected.");
    els.pickerChooseButton.disabled = !value;
    els.pickerCurrentDirButton.disabled = !(profile && profile.mode === "dir");
  }

  function updateFileCount(kind, text) {
    const node = kind === "schema" ? els.schemaCount : els.csvCount;
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

  function getStageInfo(snapshot, latestEvent) {
    if (!snapshot) {
      return { label: "Preparing" };
    }
    if (snapshot.state === "completed") {
      const completedPhase = lastResolvedPhase() || latestDataPhase();
      return completedPhase ? { label: formatPhase(completedPhase), phase: completedPhase } : { label: "Completed" };
    }

    const phase = dataPhaseForEvent(latestEvent) || latestDataPhase();
    if (phase) {
      return { label: formatPhase(phase), phase: phase };
    }
    if (snapshot.state === "failed") {
      return { label: "Failed" };
    }
    return { label: "Preparing" };
  }

  function dataPhaseForEvent(event) {
    if (!event) {
      return "";
    }
    if (isDataPhase(event.phase)) {
      return event.phase;
    }
    const metricsPhase = event.metrics && event.metrics.phase ? String(event.metrics.phase) : "";
    return isDataPhase(metricsPhase) ? metricsPhase : "";
  }

  function latestDataPhase() {
    for (let i = state.events.length - 1; i >= 0; i -= 1) {
      const phase = dataPhaseForEvent(state.events[i]);
      if (phase) {
        return phase;
      }
    }
    return "";
  }

  function lastResolvedPhase() {
    const phases = currentResolvedPhases();
    return phases.length ? phases[phases.length - 1] : "";
  }

  function isDataPhase(phase) {
    return phase === "split" || phase === "validate" || phase === "batch";
  }

  function currentResolvedPhases() {
    const final = finalResultInfo();
    const resolved = final.resolved || state.lastSubmittedResolved || state.preview.resolved;
    return resolved && resolved.plan && Array.isArray(resolved.plan.phases) ? resolved.plan.phases : [];
  }

  function finalResultInfo() {
    if (!state.result) {
      return { resolved: null, pipeline: null };
    }
    const outer = state.result.result || null;
    if (outer && (outer.resolved_config || outer.result || outer.mode)) {
      return {
        resolved: outer.resolved_config || null,
        pipeline: outer.result || null,
      };
    }
    return {
      resolved: state.result.resolved_config || null,
      pipeline: outer,
    };
  }

  function phaseInPlan(resolved, phase) {
    return Boolean(resolved && resolved.plan && Array.isArray(resolved.plan.phases) && resolved.plan.phases.indexOf(phase) >= 0);
  }

  function splitPrimaryKeyText(resolved) {
    const configured = resolved && resolved.split ? resolved.split.primary_key : "";
    return configured ? configured : "First CSV column";
  }

  function formatPhase(phase) {
    switch (phase) {
      case "run":
        return "Run";
      case "split":
        return "Split";
      case "validate":
        return "Validation";
      case "batch":
        return "Batching";
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

  function valueOrEmpty(value) {
    if (value == null) {
      return "";
    }
    return String(value);
  }

  function integerValue(value) {
    if (value == null || String(value).trim() === "") {
      return 0;
    }
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? 0 : parsed;
  }

  function listText(value) {
    return Array.isArray(value) && value.length ? value.join(" -> ") : "None";
  }

  function displayFileName(relativePath) {
    if (!relativePath) {
      return "";
    }
    const segments = String(relativePath).split("/");
    return segments[segments.length - 1] || relativePath;
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

  function deepClone(value) {
    return JSON.parse(JSON.stringify(value || {}));
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
