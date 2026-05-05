(function () {
  "use strict";

  const bootstrap = window.GVY_UI_BOOTSTRAP || {};
  const phaseOrder = ["split", "validate", "batch"];
  const schemaEditorStorageKey = "gvy.schemaEditor.savedSchema";
  const fileBrowser = window.GVYFileBrowser;
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
    schemaEditor: {
      lastSavedAt: Date.now(),
      open: false,
    },
    schemaInfer: {
      open: false,
    },
    schemaDraft: {
      open: false,
      currentPath: "",
      parentPath: "",
      entries: [],
      filter: "",
      draftName: "new.schema.json",
      selectedFile: "",
    },
    errorExplorer: {
      open: false,
    },
    reviewErrorSummary: {
      status: "idle",
      key: "",
      data: null,
      error: "",
    },
    wizard: {
      activeStep: 0,
      maxStep: 5,
    },
  };

  const els = {
    form: document.getElementById("run-form"),
    wizardCards: document.querySelectorAll(".wizard-card[data-step]"),
    wizardStepButtons: document.querySelectorAll("[data-wizard-step]"),
    wizardBackButton: document.getElementById("wizard-back-button"),
    wizardNextButton: document.getElementById("wizard-next-button"),
    wizardStepStatus: document.getElementById("wizard-step-status"),
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
    schemaInferOpenButton: document.getElementById("schema-infer-open-button"),
    schemaInferModal: document.getElementById("schema-infer-modal"),
    schemaInferBackdrop: document.getElementById("schema-infer-backdrop"),
    schemaInferCloseButton: document.getElementById("schema-infer-close-button"),
    schemaInferFrame: document.getElementById("schema-infer-frame"),
    schemaEditorOpenButton: document.getElementById("schema-editor-open-button"),
    schemaEditorNewButton: document.getElementById("schema-editor-new-button"),
    schemaEditorModal: document.getElementById("schema-editor-modal"),
    schemaEditorBackdrop: document.getElementById("schema-editor-backdrop"),
    schemaEditorTitle: document.getElementById("schema-editor-title"),
    schemaEditorSaveButton: document.getElementById("schema-editor-save-button"),
    schemaEditorSaveQuitButton: document.getElementById("schema-editor-save-quit-button"),
    schemaEditorCloseButton: document.getElementById("schema-editor-close-button"),
    schemaEditorFrame: document.getElementById("schema-editor-frame"),
    schemaDraftModal: document.getElementById("schema-draft-modal"),
    schemaDraftBackdrop: document.getElementById("schema-draft-backdrop"),
    schemaDraftCloseButton: document.getElementById("schema-draft-close-button"),
    schemaDraftPathValue: document.getElementById("schema-draft-path-value"),
    schemaDraftUpButton: document.getElementById("schema-draft-up-button"),
    schemaDraftFilterInput: document.getElementById("schema-draft-filter-input"),
    schemaDraftNameInput: document.getElementById("schema-draft-name-input"),
    schemaDraftPathPreview: document.getElementById("schema-draft-path-preview"),
    schemaDraftDirectories: document.getElementById("schema-draft-directories"),
    schemaDraftFileSelect: document.getElementById("schema-draft-file-select"),
    schemaDraftSelectionSummary: document.getElementById("schema-draft-selection-summary"),
    schemaDraftCreateButton: document.getElementById("schema-draft-create-button"),
    csvCount: document.getElementById("csv-count"),
    schemaCount: document.getElementById("schema-count"),
    csvSelectionSummary: document.getElementById("csv-selection-summary"),
    schemaSelectionSummary: document.getElementById("schema-selection-summary"),
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
    phaseTimeline: document.getElementById("phase-timeline"),
    summaryCards: document.getElementById("summary-cards"),
    reviewErrorSummary: document.getElementById("review-error-summary"),
    copyReportButton: document.getElementById("copy-report-button"),
    downloadReportButton: document.getElementById("download-report-button"),
    reportStatus: document.getElementById("report-status"),
    errorExplorerOpenButton: document.getElementById("error-explorer-open-button"),
    errorExplorerModal: document.getElementById("error-explorer-modal"),
    errorExplorerBackdrop: document.getElementById("error-explorer-backdrop"),
    errorExplorerCloseButton: document.getElementById("error-explorer-close-button"),
    errorExplorerFrame: document.getElementById("error-explorer-frame"),
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
    window.GVYOpenInferredSchema = openSchemaEditorPath;
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

    els.schemaInferOpenButton.addEventListener("click", function () {
      openSchemaInfer();
    });

    els.schemaEditorOpenButton.addEventListener("click", function () {
      openSchemaEditor("edit");
    });

    if (els.schemaEditorNewButton) {
      els.schemaEditorNewButton.addEventListener("click", function () {
        openSchemaDraftPicker();
      });
    }

    els.validateCSVOpenButton.addEventListener("click", function () {
      openPicker("validateCsv");
    });

    els.validateDirOpenButton.addEventListener("click", function () {
      openPicker("validateDir");
    });

    els.batchInputDirOpenButton.addEventListener("click", function () {
      openPicker("batchDir");
    });

    els.errorExplorerOpenButton.addEventListener("click", function () {
      openErrorExplorer();
    });
    els.copyReportButton.addEventListener("click", copyRunReport);
    els.downloadReportButton.addEventListener("click", downloadRunReport);

    configControlElements().forEach(function (control) {
      control.addEventListener("input", handleConfigControlChange);
      control.addEventListener("change", handleConfigControlChange);
    });

    els.pickerFilterInput.addEventListener("input", function () {
      const kind = state.browser.activeKind;
      if (!kind) {
        return;
      }
      state.filters[kind] = fileBrowser.normalizeFilter(els.pickerFilterInput.value);
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

    els.form.addEventListener("submit", function (event) {
      event.preventDefault();
      if (state.wizard.activeStep === 3) {
        submitRun();
      }
    });

    els.pickerChooseButton.addEventListener("click", function () {
      commitPickerSelection();
    });

    els.pickerCloseButton.addEventListener("click", closePicker);
    els.pickerBackdrop.addEventListener("click", closePicker);

    window.addEventListener("storage", function (event) {
      if (event.key === schemaEditorStorageKey) {
        applySavedSchemaEditorState(event.newValue);
      }
    });
    window.addEventListener("focus", readSavedSchemaEditorState);
    window.addEventListener("message", handleFrameMessage);
    els.schemaInferCloseButton.addEventListener("click", closeSchemaInfer);
    els.schemaInferBackdrop.addEventListener("click", closeSchemaInfer);
    els.schemaEditorSaveButton.addEventListener("click", function () {
      requestSchemaEditorSave(false);
    });
    els.schemaEditorSaveQuitButton.addEventListener("click", function () {
      requestSchemaEditorSave(true);
    });
    els.schemaEditorCloseButton.addEventListener("click", closeSchemaEditor);
    els.schemaEditorBackdrop.addEventListener("click", closeSchemaEditor);
    els.errorExplorerCloseButton.addEventListener("click", closeErrorExplorer);
    els.errorExplorerBackdrop.addEventListener("click", closeErrorExplorer);
    els.schemaDraftBackdrop.addEventListener("click", closeSchemaDraftPicker);
    els.schemaDraftCloseButton.addEventListener("click", closeSchemaDraftPicker);
    els.schemaDraftNameInput.addEventListener("input", function () {
      state.schemaDraft.draftName = els.schemaDraftNameInput.value;
      renderSchemaDraftPicker();
    });
    els.schemaDraftFilterInput.addEventListener("input", function () {
      state.schemaDraft.filter = fileBrowser.normalizeFilter(els.schemaDraftFilterInput.value);
      renderSchemaDraftPicker();
    });
    els.schemaDraftFileSelect.addEventListener("change", function () {
      state.schemaDraft.selectedFile = els.schemaDraftFileSelect.value || "";
      if (state.schemaDraft.selectedFile) {
        state.schemaDraft.draftName = displayFileName(state.schemaDraft.selectedFile);
      }
      renderSchemaDraftPicker();
    });
    els.schemaDraftUpButton.addEventListener("click", function () {
      loadSchemaDraftFileList(state.schemaDraft.parentPath || "");
    });
    els.schemaDraftCreateButton.addEventListener("click", createSchemaDraftFromPicker);
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
    const phases = selectedPhases();
    const split = phases.indexOf("split") >= 0;
    const validate = phases.indexOf("validate") >= 0;
    const batch = phases.indexOf("batch") >= 0;
    if (!phases.length) {
      return "Select at least one pipeline phase.";
    }
    if (split && !state.selected.csv) {
      return "Select a main CSV before resolving this workflow.";
    }
    if (validate && !state.selected.schema) {
      return "Select a schema JSON before resolving this workflow.";
    }
    if (validate && !split && !els.validateCSVInput.value.trim() && !els.validateDirInput.value.trim()) {
      return "Select a validation file or directory, or include the split phase.";
    }
    if (batch && !validate && !els.batchInputDirInput.value.trim()) {
      return "Select a batch input directory, or include the validate phase.";
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

    const previousRunId = state.runId;
    closeStream();
    els.wizardNextButton.disabled = true;
    const config = buildCurrentConfig();
    state.lastSubmittedConfig = deepClone(config);
    state.lastSubmittedResolved = state.preview.resolved ? deepClone(state.preview.resolved) : null;
    setFormMessage("Creating config-driven run from the current pipeline settings…", "info");
    setWizardStep(4);
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
          await syncRun(state.health.latest_run_id);
        }
        if (state.snapshot && state.snapshot.run_id) {
          setWizardStep(4);
        } else {
          setWizardStep(3);
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
      setFormMessage("Run completed. Final report is available in Review.", "ok");
      refreshHealth();
    } catch (error) {
      setFormMessage(error.message || "Run creation failed", "error");
      if (!(state.runId && state.runId !== previousRunId)) {
        setWizardStep(3);
      }
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
          resetReviewErrorSummary();
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
    const previousRunId = state.snapshot && state.snapshot.run_id;
    state.snapshot = snapshot;
    state.runId = snapshot.run_id;
    if (previousRunId !== snapshot.run_id) {
      resetReviewErrorSummary();
    }
    if (!isTerminalState(snapshot.state)) {
      state.result = null;
    }
    state.events = [];
    state.seenEvents = new Set();
    (snapshot.events || []).forEach(function (event) {
      pushEvent(event);
    });
  }

  function resetReviewErrorSummary() {
    state.reviewErrorSummary = {
      status: "idle",
      key: "",
      data: null,
      error: "",
    };
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
          if (event.type === "completed" && isRunLevelEvent(event)) {
            state.snapshot.state = "completed";
          } else if (event.type === "failed" && isRunLevelEvent(event)) {
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
    renderReviewErrorSummary();
    renderReportActions();
    renderEvents();
    updateSubmitState();
    renderPicker();
    renderSchemaEditorModal();
    renderErrorExplorerModal();
    renderSchemaDraftPicker();
  }

  function setWizardStep(step) {
    const nextStep = clampStep(step);
    if (state.wizard.activeStep === nextStep) {
      renderWizard();
      return;
    }
    state.wizard.activeStep = nextStep;
    renderWizard();
    if (nextStep === 5) {
      ensureReviewErrorSummary();
    }
  }

  function nextWizardStep() {
    if (state.wizard.activeStep === 3) {
      submitRun();
      return;
    }
    setWizardStep(state.wizard.activeStep + 1);
    if (state.wizard.activeStep === 5) {
      ensureReviewErrorSummary();
    }
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
    syncWizardNextButtonState();
    els.wizardStepStatus.textContent = "Step " + (activeStep + 1) + " of " + (state.wizard.maxStep + 1);
  }

  function syncWizardNextButtonState() {
    const activeStep = clampStep(state.wizard.activeStep);
    const confirmStep = activeStep === 3;
    const busy = Boolean(state.health && state.health.busy);
    const runningThisPage = state.snapshot && state.snapshot.state === "running";
    const previewOK = state.preview.status === "ok";
    els.wizardNextButton.textContent = confirmStep ? "Start validation run" : "Next";
    els.wizardNextButton.disabled = activeStep === state.wizard.maxStep || (confirmStep && (!previewOK || (busy && !runningThisPage)));
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
    if (kind === "schema") {
      els.schemaEditorOpenButton.disabled = !state.selected.schema;
    }
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
      els.resolvedPreview.innerHTML = previewHeroHTML("Waiting for a valid setup", state.preview.error || "Choose the input, schema, and workflow options to see the final run summary.", "muted");
      return;
    }

    const plan = resolved.plan || {};
    const inputs = resolved.inputs || {};
    const outputs = resolved.outputs || {};
    const split = resolved.split || {};
    const validation = resolved.validation || {};
    const batch = resolved.batch || {};
    const runtime = resolved.runtime || {};
    const hasSplit = phaseInPlan(resolved, "split");
    const hasValidate = phaseInPlan(resolved, "validate");
    const hasBatch = phaseInPlan(resolved, "batch");
    const phaseLabels = Array.isArray(plan.phases) ? plan.phases.map(phaseLabel).filter(Boolean) : [];
    const sections = [
      previewSectionHTML("Workflow", [
        rowHTML("Phases", phaseLabels.length ? phaseLabels.join(" -> ") : "None selected"),
        rowHTML("Resume behavior", resumePolicyText(plan.resume_policy)),
        rowHTML("Workers", workerText(runtime.workers, resolved.effective_workers)),
      ]),
    ];

    const inputRows = [];
    if (hasSplit) {
      inputRows.push(rowHTML("Main CSV", pathText(plan.split_input_csv || inputs.main_csv)));
    }
    if (hasValidate) {
      inputRows.push(rowHTML("Schema", pathText(plan.validate_schema || inputs.schema)));
      inputRows.push(rowHTML("Rows to validate", pathText(plan.validate_input_csv || plan.validate_input_dir || inputs.validate_csv || inputs.validate_dir)));
    }
    if (hasBatch) {
      inputRows.push(rowHTML("Parquet source", pathText(plan.batch_input_dir || batch.input_dir)));
    }
    sections.push(previewSectionHTML("Files used", inputRows.length ? inputRows : [
      rowHTML("Source", "No source selected"),
    ]));

    const outputRows = [];
    if (hasSplit) {
      outputRows.push(rowHTML("Split CSVs", pathText(plan.split_output_dir || outputs.split_dir)));
    }
    if (hasValidate) {
      outputRows.push(rowHTML("Valid parquet", pathText(plan.validation_success_dir || outputs.success_dir)));
      outputRows.push(rowHTML("Error CSVs", pathText(plan.validation_error_dir || outputs.error_dir)));
    }
    if (hasBatch) {
      outputRows.push(rowHTML("Batch export", pathText(plan.batch_output_dir || outputs.batch_export_dir)));
    }
    sections.push(previewSectionHTML("Output folders", outputRows.length ? outputRows : [
      rowHTML("Outputs", "No output directories for the selected phases"),
    ]));

    const settingRows = [];
    if (hasSplit) {
      settingRows.push(rowHTML("Split by", splitPrimaryKeyText(resolved)));
      settingRows.push(rowHTML("Split cache", split.reuse_cache ? "Reuse valid cached split files" : "Create split files again"));
    }
    if (hasValidate) {
      settingRows.push(rowHTML("Validation outputs", validation.clear_outputs ? "Clear existing validation outputs first" : "Keep existing validation outputs"));
      settingRows.push(rowHTML("Empty error files", validation.write_empty_error ? "Write an error file even when no rows fail" : "Only write error files when rows fail"));
    }
    if (hasBatch) {
      settingRows.push(rowHTML("Batch size", valueText(batch.size)));
      settingRows.push(rowHTML("Batch output", batch.clear_output ? "Clear existing batch export first" : "Keep existing batch export files"));
    }
    sections.push(previewSectionHTML("Important settings", settingRows.length ? settingRows : [
      rowHTML("Settings", "No extra settings for this workflow"),
    ]));

    sections.push(previewSectionHTML("Before you start", confirmationNotes(resolved, state.preview.error)));

    els.resolvedPreview.innerHTML = sections.join("");
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

    fileBrowser.renderDirectoryList(els.pickerDirectories, directories, {
      kind: kind,
      onChoose: function (path) {
        loadFileList(kind, path);
      },
    });

    fileBrowser.populateSelect(select, files, {
      selectedValue: selectedValue,
      clearWhenEmptySelection: true,
      emptyText: profile.mode === "dir"
        ? "No subdirectories match the current filter"
        : hasFileEntries(kind)
          ? "No files match the current filter"
          : "No matching files in this directory",
    });

    updateFileCount(apiKind, directories.length + " dirs · " + selectableFileEntries(kind).length + " files");
    updatePickerSelectionState();
  }

  function renderRunState() {
    const snapshot = state.snapshot;
    if (!snapshot) {
      els.runIDValue.textContent = state.runId || "Waiting for submission";
      els.runStageValue.textContent = "Not available";
      els.runProgressValue.textContent = "Not started";
      els.stageDetail.textContent = "Stage";
      els.phaseHeading.textContent = state.pendingConfigRun ? "Starting run" : state.health.busy ? "Server busy" : "Ready";
      els.phaseDetail.textContent = state.pendingConfigRun ? "Waiting for the run snapshot and progress stream." : state.health.busy ? "A run exists, but this page has not attached to its snapshot yet." : "No active run.";
      els.progressFill.style.width = "0%";
      renderPhaseTimeline(null, null);
      setBadge(els.runStateBadge, state.health.busy ? "Busy" : "No run selected", state.health.busy ? "warn" : "muted");
      return;
    }

    const latestEvent = snapshot.latest_event || newestEvent();
    const progressPercent = getProgressPercent(snapshot, latestEvent);
    const stageInfo = getStageInfo(snapshot, latestEvent);
    const phaseText = snapshot.state === "completed" ? "Complete" : stageInfo.phase ? formatPhase(stageInfo.phase) : latestEvent ? formatPhase(latestEvent.phase) : formatState(snapshot.state);
    const detail = snapshot.state === "completed" ? "All selected stages completed." : latestEvent && latestEvent.message ? latestEvent.message : describeState(snapshot.state);

    els.runIDValue.textContent = snapshot.run_id;
    els.runStageValue.textContent = totalRowsText();
    els.runProgressValue.textContent = runTimeText(snapshot);
    els.stageDetail.textContent = "Stage";
    els.phaseHeading.textContent = phaseText;
    els.phaseDetail.textContent = detail;
    els.progressFill.style.width = (progressPercent == null ? 0 : progressPercent) + "%";
    renderPhaseTimeline(snapshot, stageInfo);
    setBadge(els.runStateBadge, formatState(snapshot.state), toneForState(snapshot.state));
  }

  function renderPhaseTimeline(snapshot, stageInfo) {
    const phases = timelinePhases(snapshot);
    if (!phases.length) {
      els.phaseTimeline.innerHTML = [
        '<li class="phase-step phase-step--idle">',
        '<span class="phase-step__marker">1</span>',
        '<span class="phase-step__body">',
        '<strong>Waiting for a run</strong>',
        '<span>Configured stages will appear here when the run starts.</span>',
        "</span>",
        "</li>",
      ].join("");
      return;
    }

    const failed = snapshot && snapshot.state === "failed";
    const completed = snapshot && snapshot.state === "completed";
    const activePhase = stageInfo && stageInfo.phase ? stageInfo.phase : activeTimelinePhase(snapshot, phases);
    let activeIndex = activePhase ? phases.indexOf(activePhase) : -1;
    if (failed && activeIndex < 0) {
      activeIndex = 0;
    }

    els.phaseTimeline.innerHTML = phases.map(function (phase, index) {
      const status = phaseTimelineStatus(index, activeIndex, completed, failed);
      return [
        '<li class="phase-step phase-step--' + status.tone + '">',
        '<span class="phase-step__marker">' + (index + 1) + "</span>",
        '<span class="phase-step__body">',
        "<strong>" + escapeHTML(formatPhase(phase)) + "</strong>",
        "<span>" + escapeHTML(status.label) + "</span>",
        "</span>",
        "</li>",
      ].join("");
    }).join("");
  }

  function renderSummary() {
    const snapshot = state.snapshot;
    const final = finalResultInfo();
    const result = final.pipeline || null;
    const cards = [];
    const splitSummary = field(result, "split_summary") || {};
    const validation = field(result, "validation", "validation_dir") || {};
    const validationSummary = field(validation, "summary") || {};
    const batchSummary = field(result, "batch_summary") || {};

    if (!snapshot) {
      els.summaryCards.innerHTML = cardHTML("Report", [
        rowHTML("Status", "No run selected"),
        rowHTML("Details", "Run metrics will appear here after validation starts."),
      ]);
      return;
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

    els.summaryCards.innerHTML = cards.join("");
  }

  function renderReportActions() {
    const hasSnapshot = Boolean(state.snapshot);
    els.copyReportButton.disabled = !hasSnapshot;
    els.downloadReportButton.disabled = !hasSnapshot;
    if (!hasSnapshot && !els.reportStatus.textContent) {
      els.reportStatus.textContent = "Start a run to generate report data.";
      els.reportStatus.dataset.tone = "info";
    } else if (hasSnapshot && els.reportStatus.textContent === "Start a run to generate report data.") {
      els.reportStatus.textContent = "";
      els.reportStatus.dataset.tone = "info";
    }
  }

  function renderReviewErrorSummary() {
    const summary = state.reviewErrorSummary;
    if (!state.snapshot) {
      els.reviewErrorSummary.innerHTML = '<p class="review-error-summary__empty">Validation error patterns will appear here after a run is available.</p>';
      return;
    }
    if (summary.status === "loading") {
      els.reviewErrorSummary.innerHTML = '<p class="review-error-summary__empty">Loading validation error summary.</p>';
      return;
    }
    if (summary.status === "error") {
      els.reviewErrorSummary.innerHTML = '<p class="review-error-summary__empty">' + escapeHTML(summary.error || "Could not load validation error summary.") + "</p>";
      return;
    }
    if (!summary.data) {
      els.reviewErrorSummary.innerHTML = '<p class="review-error-summary__empty">Open Review to load grouped validation errors and examples.</p>';
      return;
    }
    if (!summary.data.issues.length) {
      els.reviewErrorSummary.innerHTML = '<p class="review-error-summary__empty">No validation error patterns were found in ' + escapeHTML(summary.data.path || currentErrorDir()) + ".</p>";
      return;
    }
    els.reviewErrorSummary.innerHTML = summary.data.issues.map(reviewIssueHTML).join("");
  }

  function ensureReviewErrorSummary() {
    if (!state.snapshot) {
      return;
    }
    const key = reviewErrorSummaryKey();
    if (state.reviewErrorSummary.key === key && (state.reviewErrorSummary.status === "loading" || state.reviewErrorSummary.status === "ok")) {
      return;
    }
    loadReviewErrorSummary(key);
  }

  function reviewErrorSummaryKey() {
    return (state.snapshot && state.snapshot.run_id ? state.snapshot.run_id : "run") + "|" + currentErrorDir();
  }

  async function loadReviewErrorSummary(key) {
    state.reviewErrorSummary = {
      status: "loading",
      key: key,
      data: null,
      error: "",
    };
    renderReviewErrorSummary();

    try {
      const path = currentErrorDir();
      const report = await fetchErrorReport({ path: path, limit: 1 });
      const primaryKey = reviewPrimaryKeyName(report);
      const issues = await reviewIssuesFromMessages(path, report, primaryKey);
      state.reviewErrorSummary = {
        status: "ok",
        key: key,
        data: {
          path: report.relative_path || path,
          primaryKey: primaryKey,
          matchedRows: report.matched_rows || 0,
          issues: issues,
        },
        error: "",
      };
      renderReviewErrorSummary();
    } catch (error) {
      state.reviewErrorSummary = {
        status: "error",
        key: key,
        data: null,
        error: error.message || "Could not load validation error summary.",
      };
      renderReviewErrorSummary();
    }
  }

  async function reviewIssuesFromMessages(path, report, primaryKey) {
    const messages = uniqueReviewMessages(report.messages).slice(0, 8);
    return Promise.all(messages.map(async function (message) {
      const examplesReport = await fetchErrorReport({
        path: path,
        field: message.field || "",
        q: queryForErrorPattern(message.message || ""),
        limit: 50,
      });
      const examples = reviewExamples(examplesReport.samples || [], message.field || "", primaryKey);
      return {
        field: message.field || "Unspecified",
        message: message.message || "Validation error",
        count: message.count || 0,
        examples: examples,
      };
    }));
  }

  async function fetchErrorReport(options) {
    const params = new URLSearchParams();
    params.set("path", cleanRelativePath(options.path || currentErrorDir()) || "errors");
    params.set("limit", String(options.limit || 5));
    if (options.offset) {
      params.set("offset", String(options.offset));
    }
    if (options.field) {
      params.set("field", options.field);
    }
    if (options.q) {
      params.set("q", options.q);
    }
    const response = await fetch("/api/errors/report?" + params.toString());
    const payload = await parseJSON(response);
    if (!response.ok) {
      throw new Error(payload && payload.message ? payload.message : "Could not load error report");
    }
    return payload || {};
  }

  function reviewPrimaryKeyName(report) {
    const final = finalResultInfo();
    const result = final.pipeline || {};
    const resolved = final.resolved || state.lastSubmittedResolved || state.preview.resolved || {};
    const configured = result.split_primary_key || (resolved.split && resolved.split.primary_key) || els.splitPrimaryKeyInput.value.trim();
    if (configured) {
      return configured;
    }
    const sample = report && Array.isArray(report.samples) && report.samples.length ? report.samples[0] : null;
    const columns = sampleColumnsForReview(sample);
    return columns.length ? columns[0].name : "Primary key";
  }

  function reviewExamples(samples, fieldName, primaryKey) {
    const seen = new Set();
    const examples = [];
    samples.forEach(function (sample) {
      const values = sample.values || {};
      const columns = sampleColumnsForReview(sample);
      const errored = columns.find(function (column) {
        return column.name === fieldName;
      }) || columns.find(function (column) {
        return column.errored;
      }) || { name: fieldName || "Value", value: "" };
      const primary = Object.prototype.hasOwnProperty.call(values, primaryKey)
        ? values[primaryKey]
        : columns.length ? columns[0].value : "";
      const example = {
        primaryKey: primaryKey || "Primary key",
        primaryValue: primary,
        field: errored.name || fieldName || "Value",
        value: errored.value || "",
      };
      const key = [example.primaryValue, example.field, example.value].join("\x00");
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      examples.push(example);
    });
    return examples.slice(0, 5);
  }

  function uniqueReviewMessages(messages) {
    const seen = new Set();
    const out = [];
    (Array.isArray(messages) ? messages : []).forEach(function (message) {
      const field = message.field || "Unspecified";
      const text = message.message || "Validation error";
      const key = field.toLowerCase() + "\x00" + text.toLowerCase();
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      out.push(message);
    });
    return out;
  }

  function sampleColumnsForReview(sample) {
    if (!sample) {
      return [];
    }
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

  function reviewIssueHTML(issue) {
    return [
      '<article class="review-issue">',
      '<div class="review-issue__head">',
      '<div><h4>' + escapeHTML(issue.field) + '</h4><p class="review-issue__message">' + escapeHTML(issue.message) + '</p></div>',
      '<span class="badge badge--warn">' + escapeHTML(compactNumber(issue.count)) + ' rows</span>',
      '</div>',
      '<dl class="review-examples">',
      issue.examples.length ? issue.examples.map(reviewExampleHTML).join("") : '<p class="review-error-summary__empty">No row examples available for this pattern.</p>',
      '</dl>',
      '</article>',
    ].join("");
  }

  function reviewExampleHTML(example) {
    return [
      '<div class="review-example">',
      '<div><dt>' + escapeHTML(example.primaryKey || "Primary key") + '</dt><dd>' + escapeHTML(displayCellValue(example.primaryValue)) + '</dd></div>',
      '<div><dt>' + escapeHTML(example.field || "Value") + '</dt><dd>' + escapeHTML(displayCellValue(example.value)) + '</dd></div>',
      '</div>',
    ].join("");
  }

  function queryForErrorPattern(message) {
    return String(message || "").replace(/<value>/g, "").replace(/\s+/g, " ").replace(/:\s*$/, "").trim();
  }

  async function copyRunReport() {
    if (!state.snapshot) {
      setReportStatus("Start a run before copying a report.", "warn");
      return;
    }
    if (!state.reviewErrorSummary.data && state.reviewErrorSummary.status !== "loading") {
      await loadReviewErrorSummary(reviewErrorSummaryKey());
    }
    const text = buildRunReportText();
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        copyTextFallback(text);
      }
      setReportStatus("Report summary copied.", "ok");
    } catch (error) {
      setReportStatus("Could not copy the report summary.", "error");
    }
  }

  async function downloadRunReport() {
    if (!state.snapshot) {
      setReportStatus("Start a run before downloading a report.", "warn");
      return;
    }
    if (!state.reviewErrorSummary.data && state.reviewErrorSummary.status !== "loading") {
      await loadReviewErrorSummary(reviewErrorSummaryKey());
    }
    const blob = new Blob([JSON.stringify(buildRunReportData(), null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = (state.snapshot.run_id || "gvy-run") + ".report.json";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    setReportStatus("JSON report downloaded.", "ok");
  }

  function buildRunReportData() {
    const final = finalResultInfo();
    return {
      run: state.snapshot || null,
      resolved_config: final.resolved || state.lastSubmittedResolved || state.preview.resolved || null,
      result: final.pipeline || null,
      error_summary: state.reviewErrorSummary.data || null,
      events: state.events || [],
    };
  }

  function buildRunReportText() {
    const snapshot = state.snapshot || {};
    const final = finalResultInfo();
    const result = final.pipeline || {};
    const splitSummary = field(result, "split_summary") || {};
    const validation = field(result, "validation", "validation_dir") || {};
    const validationSummary = field(validation, "summary") || {};
    const batchSummary = field(result, "batch_summary") || {};
    return [
      "GVY Run Report",
      "Run ID: " + valueText(snapshot.run_id),
      "Status: " + formatState(snapshot.state),
      "Runtime: " + runTimeText(snapshot),
      "Rows validated: " + totalRowsText(),
      "Split rows: " + numberText(field(splitSummary, "total_rows", "TotalRows")),
      "Validation valid rows: " + numberText(field(validationSummary, "valid_rows", "ValidRows")),
      "Validation invalid rows: " + numberText(field(validationSummary, "invalid_rows", "InvalidRows")),
      "Batch rows written: " + numberText(field(batchSummary, "total_rows", "TotalRows")),
      "Final error: " + valueText((state.result && state.result.final_error) || snapshot.final_error),
      "",
      buildErrorSummaryText(),
    ].join("\n");
  }

  function buildErrorSummaryText() {
    const data = state.reviewErrorSummary.data;
    if (!data || !Array.isArray(data.issues)) {
      return "Validation error summary: Not loaded";
    }
    if (!data.issues.length) {
      return "Validation error summary: No error patterns found";
    }
    const lines = ["Validation error summary:"];
    data.issues.forEach(function (issue, index) {
      lines.push("");
      lines.push(String(index + 1) + ". " + issue.field + " - " + issue.message + " (" + issue.count + " rows)");
      issue.examples.forEach(function (example) {
        lines.push("   - " + example.primaryKey + "=" + displayCellValue(example.primaryValue) + "; " + example.field + "=" + displayCellValue(example.value));
      });
    });
    return lines.join("\n");
  }

  function copyTextFallback(text) {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.left = "-9999px";
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand("copy");
    textarea.remove();
  }

  function setReportStatus(message, tone) {
    els.reportStatus.textContent = message;
    els.reportStatus.dataset.tone = tone || "info";
  }

  function currentErrorDir() {
    const final = finalResultInfo();
    const resolved = final.resolved || state.lastSubmittedResolved || state.preview.resolved;
    const planDir = resolved && resolved.plan ? resolved.plan.validation_error_dir : "";
    const outputDir = resolved && resolved.outputs ? resolved.outputs.error_dir : "";
    return planDir || outputDir || els.errorDirInput.value || "errors";
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
    syncWizardNextButtonState();
  }

  function filteredFiles(kind) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    return fileBrowser.filteredEntries(state.browser[profile.apiKind].entries, {
      filter: state.filters[kind],
      wantDirectory: false,
      exclude: function (entry) {
        return kind === "schema" && entry.relative_path === "schema.example.json";
      },
    });
  }

  function filteredDirectories(kind) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    return fileBrowser.filteredEntries(state.browser[profile.apiKind].entries, {
      filter: state.filters[kind],
      wantDirectory: true,
    });
  }

  function fileEntries(kind) {
    const profile = pickerProfiles[kind] || pickerProfiles.csv;
    return fileBrowser.filteredEntries(state.browser[profile.apiKind].entries, {
      wantDirectory: false,
    });
  }

  function selectableFileEntries(kind) {
    return fileEntries(kind).filter(function (entry) {
      return !(kind === "schema" && entry.relative_path === "schema.example.json");
    });
  }

  function directoryEntries(kind) {
    const apiKind = pickerProfiles[kind] ? pickerProfiles[kind].apiKind : kind;
    return fileBrowser.filteredEntries(state.browser[apiKind].entries, {
      wantDirectory: true,
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

  function openSchemaInfer() {
    const params = new URLSearchParams();
    params.set("embed", "1");
    params.set("mode", "infer");
    if (state.selected.csv) {
      params.set("csv", state.selected.csv);
    }
    state.schemaEditor.open = true;
    els.schemaEditorTitle.textContent = "Generate Schema";
    els.schemaEditorFrame.src = "/schema-workbench?" + params.toString();
    renderSchemaEditorModal();
  }

  function closeSchemaInfer() {
    state.schemaInfer.open = false;
    els.schemaInferFrame.removeAttribute("src");
    renderSchemaInferModal();
  }

  function renderSchemaInferModal() {
    els.schemaInferModal.hidden = !state.schemaInfer.open;
  }

  function handleFrameMessage(event) {
    if (event.origin !== window.location.origin) {
      return;
    }
    const data = event.data || {};
    if (data.type === "gvy:schema-infer-open-schema") {
      const path = cleanRelativePath(data.path || "");
      if (!path) {
        return;
      }
      openSchemaEditorPath(path);
      return;
    }

    if (data.type !== "gvy:schema-saved" && data.type !== "gvy:schema-saved-and-close") {
      return;
    }
    const path = cleanRelativePath(data.path || "");
    if (!path) {
      return;
    }
    const savedAt = Number(data.saved_at || Date.now());
    if (Number.isFinite(savedAt)) {
      state.schemaEditor.lastSavedAt = Math.max(state.schemaEditor.lastSavedAt, savedAt);
    }
    if (data.type === "gvy:schema-saved-and-close") {
      closeSchemaEditor();
      if (data.advance && state.wizard.activeStep === 1) {
        setWizardStep(2);
      }
      selectSchemaPath(path).catch(function () {
        state.selected.schema = path;
        render();
      });
      return;
    }
    selectSchemaPath(path);
  }

  function openSchemaEditor(mode) {
    const params = new URLSearchParams();
    params.set("embed", "1");
    if (mode === "new") {
      params.set("mode", "new");
    } else if (state.selected.schema) {
      params.set("path", state.selected.schema);
    } else {
      params.set("mode", "load");
    }
    addSelectedCSVParam(params);
    state.schemaEditor.open = true;
    els.schemaEditorTitle.textContent = "Schema Editor";
    els.schemaEditorFrame.src = "/schema-workbench?" + params.toString();
    renderSchemaEditorModal();
  }

  async function openSchemaEditorPath(path) {
    const clean = cleanRelativePath(path);
    if (!clean) {
      return;
    }
    closeSchemaInfer();
    await selectSchemaPath(clean);
    const params = new URLSearchParams();
    params.set("embed", "1");
    params.set("path", clean);
    addSelectedCSVParam(params);
    state.schemaEditor.open = true;
    els.schemaEditorTitle.textContent = "Schema Editor";
    els.schemaEditorFrame.src = "/schema-workbench?" + params.toString();
    renderSchemaEditorModal();
  }

  function openSchemaEditorDraft(path) {
    const clean = cleanRelativePath(path);
    if (!clean) {
      return;
    }
    const params = new URLSearchParams();
    params.set("embed", "1");
    params.set("draft", clean);
    addSelectedCSVParam(params);
    state.schemaEditor.open = true;
    els.schemaEditorTitle.textContent = "Schema Editor";
    els.schemaEditorFrame.src = "/schema-workbench?" + params.toString();
    renderSchemaEditorModal();
  }

  function addSelectedCSVParam(params) {
    if (state.selected.csv) {
      params.set("csv", state.selected.csv);
    }
  }

  function requestSchemaEditorSave(closeAfter) {
    if (!state.schemaEditor.open || !els.schemaEditorFrame.contentWindow) {
      return;
    }
    els.schemaEditorFrame.contentWindow.postMessage({
      type: "gvy:schema-save-request",
      close: Boolean(closeAfter),
    }, window.location.origin);
  }

  function closeSchemaEditor() {
    state.schemaEditor.open = false;
    els.schemaEditorFrame.removeAttribute("src");
    renderSchemaEditorModal();
    readSavedSchemaEditorState();
  }

  function renderSchemaEditorModal() {
    els.schemaEditorModal.hidden = !state.schemaEditor.open;
  }

  function openErrorExplorer() {
    const params = new URLSearchParams();
    params.set("embed", "1");
    params.set("path", cleanRelativePath(currentErrorDir()));
    state.errorExplorer.open = true;
    els.errorExplorerFrame.src = "/error-explorer?" + params.toString();
    renderErrorExplorerModal();
  }

  function closeErrorExplorer() {
    state.errorExplorer.open = false;
    els.errorExplorerFrame.removeAttribute("src");
    renderErrorExplorerModal();
  }

  function renderErrorExplorerModal() {
    els.errorExplorerModal.hidden = !state.errorExplorer.open;
  }

  function openSchemaDraftPicker() {
    state.schemaDraft.open = true;
    state.schemaDraft.selectedFile = "";
    state.schemaDraft.filter = "";
    state.schemaDraft.draftName = normalizedDraftName(state.schemaDraft.draftName) || "new.schema.json";
    if (!state.schemaDraft.entries.length) {
      loadSchemaDraftFileList(state.schemaDraft.currentPath);
    }
    renderSchemaDraftPicker();
  }

  function closeSchemaDraftPicker() {
    state.schemaDraft.open = false;
    renderSchemaDraftPicker();
  }

  async function loadSchemaDraftFileList(path) {
    try {
      const params = new URLSearchParams();
      params.set("kind", "schema");
      if (path) {
        params.set("path", path);
      }
      const response = await fetch("/api/files?" + params.toString());
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load schema files");
      }
      state.schemaDraft.currentPath = payload.current_path || "";
      state.schemaDraft.parentPath = payload.parent_path || "";
      state.schemaDraft.entries = payload.entries || [];
      state.schemaDraft.selectedFile = "";
      renderSchemaDraftPicker();
    } catch (error) {
      state.schemaDraft.entries = [];
      setFormMessage(error.message || "Could not load schema folders", "error");
      renderSchemaDraftPicker();
    }
  }

  function renderSchemaDraftPicker() {
    els.schemaDraftModal.hidden = !state.schemaDraft.open;
    if (!state.schemaDraft.open) {
      return;
    }

    const directories = filteredSchemaDraftEntries(true);
    const files = filteredSchemaDraftEntries(false);
    const draftPath = schemaDraftRelativePath();

    els.schemaDraftNameInput.value = state.schemaDraft.draftName;
    els.schemaDraftFilterInput.value = state.schemaDraft.filter;
    els.schemaDraftPathValue.textContent = "/" + (state.schemaDraft.currentPath || "");
    els.schemaDraftUpButton.disabled = !state.schemaDraft.currentPath && !state.schemaDraft.parentPath;
    els.schemaDraftPathPreview.textContent = draftPath ? "Target: " + draftPath : "Enter a schema filename.";
    els.schemaDraftSelectionSummary.textContent = draftPath || "No filename set.";
    els.schemaDraftCreateButton.disabled = !draftPath;

    fileBrowser.renderDirectoryList(els.schemaDraftDirectories, directories, {
      onChoose: loadSchemaDraftFileList,
    });

    fileBrowser.populateSelect(els.schemaDraftFileSelect, files, {
      selectedValue: state.schemaDraft.selectedFile,
      emptyText: "No schema JSON files in this folder",
    });
  }

  function filteredSchemaDraftEntries(wantDirectory) {
    return fileBrowser.filteredEntries(state.schemaDraft.entries, {
      filter: state.schemaDraft.filter,
      wantDirectory: wantDirectory,
    });
  }

  function createSchemaDraftFromPicker() {
    const draftPath = schemaDraftRelativePath();
    if (!draftPath) {
      return;
    }
    closeSchemaDraftPicker();
    openSchemaEditorDraft(draftPath);
  }

  function schemaDraftRelativePath() {
    return fileBrowser.relativeDraftPath(state.schemaDraft.currentPath, state.schemaDraft.draftName, "json");
  }

  function normalizedDraftName(value) {
    return fileBrowser.normalizedFilename(value, "json");
  }

  function readSavedSchemaEditorState() {
    let value = "";
    try {
      value = window.localStorage.getItem(schemaEditorStorageKey) || "";
    } catch (error) {
      return;
    }
    applySavedSchemaEditorState(value);
  }

  function applySavedSchemaEditorState(value) {
    if (!value) {
      return;
    }
    let payload = null;
    try {
      payload = JSON.parse(value);
    } catch (error) {
      return;
    }
    const savedAt = Number(payload && payload.saved_at);
    const path = String(payload && payload.path || "").trim();
    if (!path || !Number.isFinite(savedAt) || savedAt <= state.schemaEditor.lastSavedAt) {
      return;
    }
    state.schemaEditor.lastSavedAt = savedAt;
    selectSchemaPath(path);
  }

  async function selectSchemaPath(relativePath) {
    const clean = String(relativePath || "").replace(/^\/+/, "");
    if (!clean) {
      return;
    }
    clearFormMessage();
    await loadFileList("schema", dirName(clean));
    state.selected.schema = clean;
    render();
    scheduleResolvePreview(0);
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
    if (profile.target === "selectedCsv") {
      setWizardStep(1);
    }
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

  function isRunLevelEvent(event) {
    return Boolean(event && event.phase === "run");
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

  function timelinePhases(snapshot) {
    const resolvedPhases = currentResolvedPhases().filter(isDataPhase);
    if (resolvedPhases.length) {
      return resolvedPhases;
    }
    const eventPhases = [];
    state.events.forEach(function (event) {
      const phase = dataPhaseForEvent(event);
      if (phase && eventPhases.indexOf(phase) < 0) {
        eventPhases.push(phase);
      }
    });
    if (eventPhases.length) {
      return phaseOrder.filter(function (phase) {
        return eventPhases.indexOf(phase) >= 0;
      });
    }
    return snapshot ? phaseOrder.slice() : selectedPhases();
  }

  function activeTimelinePhase(snapshot, phases) {
    const phase = latestDataPhase();
    if (phase && phases.indexOf(phase) >= 0) {
      return phase;
    }
    if (snapshot && snapshot.state === "queued") {
      return phases[0] || "";
    }
    return "";
  }

  function phaseTimelineStatus(index, activeIndex, completed, failed) {
    if (completed) {
      return { tone: "done", label: "Complete" };
    }
    if (failed && index === activeIndex) {
      return { tone: "failed", label: "Needs attention" };
    }
    if (activeIndex < 0) {
      return index === 0 ? { tone: "current", label: "Waiting to start" } : { tone: "waiting", label: "Waiting" };
    }
    if (index < activeIndex) {
      return { tone: "done", label: "Complete" };
    }
    if (index === activeIndex) {
      return { tone: "current", label: failed ? "Needs attention" : "In progress" };
    }
    return { tone: "waiting", label: "Waiting" };
  }

  function runTimeText(snapshot) {
    const seconds = runDurationSeconds(snapshot);
    return seconds == null ? "Not started" : formatDurationShort(seconds);
  }

  function totalRowsText() {
    const rows = rowsValidatedCount();
    if (rows == null) {
      return "Not available";
    }
    return compactNumber(rows);
  }

  function runDurationSeconds(snapshot) {
    if (!snapshot) {
      return null;
    }
    const startValue = snapshot.started_at || snapshot.created_at;
    if (!startValue) {
      return null;
    }
    const start = new Date(startValue);
    if (Number.isNaN(start.getTime())) {
      return null;
    }
    const endValue = snapshot.finished_at || null;
    const end = endValue ? new Date(endValue) : new Date();
    if (Number.isNaN(end.getTime())) {
      return null;
    }
    return Math.max(0, (end.getTime() - start.getTime()) / 1000);
  }

  function rowsValidatedCount() {
    const final = finalResultInfo();
    const result = final.pipeline || {};
    const validationDir = result.validation_dir || field(result, "validation", "Validation");
    const validationFile = result.validation_file || {};
    const dirSummary = validationDir && validationDir.summary ? validationDir.summary : {};
    const fileStats = validationFile && validationFile.stats ? validationFile.stats : {};
    const finalRows = metricNumber(
      field(dirSummary, "total_rows", "TotalRows"),
      field(fileStats, "total_rows", "TotalRows")
    );
    if (finalRows != null) {
      return finalRows;
    }

    for (let i = state.events.length - 1; i >= 0; i -= 1) {
      const event = state.events[i];
      if (dataPhaseForEvent(event) !== "validate") {
        continue;
      }
      const metrics = event.metrics || {};
      const validRows = metricNumber(field(metrics, "valid_rows", "ValidRows"));
      const invalidRows = metricNumber(field(metrics, "invalid_rows", "InvalidRows"));
      const combinedRows = validRows != null || invalidRows != null ? (validRows || 0) + (invalidRows || 0) : null;
      const eventRows = metricNumber(field(metrics, "total_rows", "TotalRows"), combinedRows);
      if (eventRows != null) {
        return eventRows;
      }
    }
    return null;
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

  function metricNumber() {
    for (let index = 0; index < arguments.length; index += 1) {
      const value = arguments[index];
      if (value == null || value === "") {
        continue;
      }
      const number = Number(value);
      if (Number.isFinite(number)) {
        return number;
      }
    }
    return null;
  }

  function compactNumber(value) {
    const number = metricNumber(value);
    if (number == null) {
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

  function formatDurationShort(seconds) {
    const totalSeconds = Math.max(0, Math.round(Number(seconds) || 0));
    if (totalSeconds < 60) {
      return totalSeconds + "s";
    }
    const minutes = Math.floor(totalSeconds / 60);
    const remainingSeconds = totalSeconds % 60;
    if (minutes < 60) {
      return minutes + "m " + pad2(remainingSeconds) + "s";
    }
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    if (hours < 24) {
      return hours + "h " + pad2(remainingMinutes) + "m";
    }
    const days = Math.floor(hours / 24);
    const remainingHours = hours % 24;
    return days + "d " + pad2(remainingHours) + "h";
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function valueText(value) {
    if (value == null || value === "") {
      return "Not available";
    }
    return String(value);
  }

  function displayCellValue(value) {
    if (value == null || value === "") {
      return "(blank)";
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

  function booleanText(value) {
    return value ? "Yes" : "No";
  }

  function pathText(value) {
    const text = valueText(value);
    return text === "Not available" ? "Not selected" : text;
  }

  function phaseLabel(phase) {
    switch (phase) {
      case "split":
        return "Split";
      case "validate":
        return "Validate";
      case "batch":
        return "Batch export";
      default:
        return valueText(phase);
    }
  }

  function resumePolicyText(value) {
    switch (value) {
      case "reuse_valid_outputs":
        return "Reuse valid existing outputs where possible";
      case "start_at_first_missing":
        return "Resume from the first missing output";
      case "run_all":
        return "Run every selected phase from scratch";
      default:
        return valueText(value);
    }
  }

  function workerText(configured, effective) {
    const effectiveText = valueText(effective);
    if (!configured) {
      return "Auto (" + effectiveText + " effective)";
    }
    return String(configured) + " configured (" + effectiveText + " effective)";
  }

  function confirmationNotes(resolved, error) {
    if (error) {
      return [
        rowHTML("Blocked", error),
      ];
    }
    const validation = resolved.validation || {};
    const batch = resolved.batch || {};
    const notes = [
      rowHTML("Validation", "The server has resolved this setup successfully."),
    ];
    if (validation.clear_outputs || batch.clear_output) {
      notes.push(rowHTML("Output clearing", "One or more clear-output settings are enabled."));
    } else {
      notes.push(rowHTML("Output clearing", "Existing output folders will not be cleared first."));
    }
    notes.push(rowHTML("Next action", "Start validation run will begin immediately."));
    return notes;
  }

  function resolverStatusText() {
    switch (state.preview.status) {
      case "pending":
        return "Resolving";
      case "ok":
        return "Ready";
      case "error":
        return "Invalid";
      default:
        return "Waiting";
    }
  }

  function displayFileName(relativePath) {
    return fileBrowser.baseName(relativePath);
  }

  function dirName(path) {
    return fileBrowser.dirName(path);
  }

  function cleanRelativePath(path) {
    return fileBrowser.cleanRelativePath(path);
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

  function previewSectionHTML(title, rows) {
    return [
      '<div class="preview-group">',
      '<dt class="preview-group__title">' + escapeHTML(title) + "</dt>",
      "<dd>",
      '<dl class="preview-group__items">',
      rows.join(""),
      "</dl>",
      "</dd>",
      "</div>",
    ].join("");
  }

  function previewHeroHTML(title, detail, tone) {
    return [
      '<div class="preview-group preview-group--hero preview-group--' + escapeHTML(tone || "muted") + '">',
      '<dt class="preview-group__title">' + escapeHTML(title) + "</dt>",
      '<dd class="preview-group__summary">' + escapeHTML(detail) + "</dd>",
      "</div>",
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
