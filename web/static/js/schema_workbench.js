(function () {
  "use strict";

  const blankSchema = { fields: [] };
  const supportedTypes = ["string", "int", "float", "date", "datetime"];
  const schemaEditorStorageKey = "gvy.schemaEditor.savedSchema";
  const fileBrowser = window.GVYFileBrowser;

  const state = {
    currentPath: "",
    parentPath: "",
    entries: [],
    selectedFile: "",
    filter: "",
    pickerOpen: false,
    pickerMode: "load",
    draftName: "new.schema.json",
    loadedPath: "",
    schema: deepClone(blankSchema),
    selectedCSV: "",
    activeIndex: -1,
    dirty: false,
    message: "",
    messageTone: "",
  };

  const els = {
    workbench: document.getElementById("schema-workbench"),
    refreshButton: document.getElementById("refresh-button"),
    pickerModal: document.getElementById("schema-picker-modal"),
    pickerBackdrop: document.getElementById("picker-backdrop"),
    pickerTitle: document.getElementById("picker-title"),
    pickerSubtitle: document.getElementById("picker-subtitle"),
    pickerCloseButton: document.getElementById("picker-close-button"),
    pickerFilterInput: document.getElementById("picker-filter-input"),
    pickerSelectionSummary: document.getElementById("picker-selection-summary"),
    pickerChooseButton: document.getElementById("picker-choose-button"),
    pickerListLabel: document.getElementById("picker-list-label"),
    draftLocationPanel: document.getElementById("draft-location-panel"),
    schemaNameLabel: document.getElementById("schema-name-label"),
    draftNameInput: document.getElementById("draft-name-input"),
    draftPathPreview: document.getElementById("draft-path-preview"),
    currentPath: document.getElementById("current-path"),
    upButton: document.getElementById("up-button"),
    directoryList: document.getElementById("directory-list"),
    schemaFileSelect: document.getElementById("schema-file-select"),
    loadButton: document.getElementById("load-button"),
    newButton: document.getElementById("new-button"),
    saveButton: document.getElementById("save-button"),
    message: document.getElementById("message"),
    fieldCount: document.getElementById("field-count"),
    addFieldButton: document.getElementById("add-field-button"),
    fieldTableBody: document.getElementById("field-table-body"),
    deleteFieldButton: document.getElementById("delete-field-button"),
    fieldDetailSubtitle: document.getElementById("field-detail-subtitle"),
    fieldEditorEmpty: document.getElementById("field-editor-empty"),
    fieldEditor: document.getElementById("field-editor"),
    fieldNameInput: document.getElementById("field-name-input"),
    fieldParquetInput: document.getElementById("field-parquet-input"),
    fieldTypeInput: document.getElementById("field-type-input"),
    fieldMinLengthInput: document.getElementById("field-min-length-input"),
    fieldDefaultInput: document.getElementById("field-default-input"),
    fieldOverrideInput: document.getElementById("field-override-input"),
    fieldRequiredInput: document.getElementById("field-required-input"),
    fieldLowerInput: document.getElementById("field-lower-input"),
    fieldNonZeroInput: document.getElementById("field-non-zero-input"),
    fieldAllowedAdd: document.getElementById("field-allowed-add"),
    fieldAllowedRows: document.getElementById("field-allowed-rows"),
    fieldInlineReplaceAdd: document.getElementById("field-inline-replace-add"),
    fieldInlineReplaceRows: document.getElementById("field-inline-replace-rows"),
    fieldDateFormatsInput: document.getElementById("field-date-formats-input"),
    fieldDatetimeFormatsInput: document.getElementById("field-datetime-formats-input"),
    typeSpecificOptions: Array.from(document.querySelectorAll("[data-field-types]")),
  };

  function init() {
    bindEvents();
    render();
    if (!applyStartupIntent()) {
      loadFileList("");
    }
  }

  function bindEvents() {
    els.refreshButton.addEventListener("click", function () {
      loadFileList(state.currentPath);
    });
    els.pickerBackdrop.addEventListener("click", closeSchemaPicker);
    els.pickerCloseButton.addEventListener("click", closeSchemaPicker);
    els.pickerFilterInput.addEventListener("input", function () {
      state.filter = fileBrowser.normalizeFilter(els.pickerFilterInput.value);
      renderPicker();
    });
    els.draftNameInput.addEventListener("input", function () {
      state.draftName = els.draftNameInput.value;
      renderPicker();
    });
    els.upButton.addEventListener("click", function () {
      loadFileList(state.parentPath || "");
    });
    els.schemaFileSelect.addEventListener("change", function () {
      state.selectedFile = els.schemaFileSelect.value || "";
      if (state.pickerMode === "save" && state.selectedFile) {
        state.draftName = baseName(state.selectedFile);
      }
      render();
    });
    els.schemaFileSelect.addEventListener("dblclick", function () {
      if (state.pickerMode === "load") {
        loadSelectedSchema();
      }
    });
    els.loadButton.addEventListener("click", function () {
      openSchemaPicker("load");
    });
    els.pickerChooseButton.addEventListener("click", function () {
      if (state.pickerMode === "new") {
        createDraftFromPicker();
        return;
      }
      if (state.pickerMode === "save") {
        saveSchemaFromPicker();
        return;
      }
      loadSelectedSchema();
    });
    els.newButton.addEventListener("click", function () {
      openSchemaPicker("new");
    });
    els.saveButton.addEventListener("click", openSavePicker);
    els.addFieldButton.addEventListener("click", addField);
    els.deleteFieldButton.addEventListener("click", deleteActiveField);
    els.fieldAllowedAdd.addEventListener("click", addAllowedValueRowAndUpdate);
    els.fieldAllowedRows.addEventListener("input", updateActiveFieldFromForm);
    els.fieldAllowedRows.addEventListener("change", updateActiveFieldFromForm);
    els.fieldAllowedRows.addEventListener("click", function (event) {
      const button = event.target.closest("[data-allowed-value-remove]");
      if (!button) {
        return;
      }
      const row = button.closest(".allowed-value-row");
      if (row) {
        row.remove();
      }
      updateActiveFieldFromForm();
    });
    els.fieldInlineReplaceAdd.addEventListener("click", addInlineReplaceRowAndUpdate);
    els.fieldInlineReplaceRows.addEventListener("input", updateActiveFieldFromForm);
    els.fieldInlineReplaceRows.addEventListener("change", updateActiveFieldFromForm);
    els.fieldInlineReplaceRows.addEventListener("click", function (event) {
      const button = event.target.closest("[data-inline-replace-remove]");
      if (!button) {
        return;
      }
      const row = button.closest(".inline-replace-row");
      if (row) {
        row.remove();
      }
      updateActiveFieldFromForm();
    });

    fieldControls().forEach(function (control) {
      control.addEventListener("input", updateActiveFieldFromForm);
      control.addEventListener("change", updateActiveFieldFromForm);
    });
  }

  function fieldControls() {
    return [
      els.fieldNameInput,
      els.fieldParquetInput,
      els.fieldTypeInput,
      els.fieldMinLengthInput,
      els.fieldDefaultInput,
      els.fieldOverrideInput,
      els.fieldRequiredInput,
      els.fieldLowerInput,
      els.fieldNonZeroInput,
      els.fieldDateFormatsInput,
      els.fieldDatetimeFormatsInput,
    ];
  }

  function applyStartupIntent() {
    const params = new URLSearchParams(window.location.search || "");
    const mode = params.get("mode") || "";
    const path = cleanRelativePath(params.get("path") || "");
    const draft = cleanRelativePath(params.get("draft") || "");
    state.selectedCSV = cleanRelativePath(params.get("csv") || "");

    if (path) {
      loadSchemaByPath(path);
      return true;
    }
    if (draft) {
      loadFileList(dirName(draft)).then(function () {
        newDraft(draft);
      });
      return true;
    }
    if (mode === "new") {
      loadFileList("").then(function () {
        openSchemaPicker("new");
      });
      return true;
    } else if (mode === "load") {
      loadFileList("").then(function () {
        openSchemaPicker("load");
      });
      return true;
    }
    return false;
  }

  async function loadSchemaByPath(path) {
    const relativePath = cleanRelativePath(path);
    if (!relativePath) {
      return;
    }
    state.selectedFile = relativePath;
    await loadFileList(dirName(relativePath));
    state.selectedFile = relativePath;
    await loadSelectedSchema();
  }

  async function loadFileList(path) {
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
      state.currentPath = payload.current_path || "";
      state.parentPath = payload.parent_path || "";
      state.entries = payload.entries || [];
      if (state.selectedFile && !state.entries.some(function (entry) { return entry.relative_path === state.selectedFile; })) {
        state.selectedFile = "";
      }
      setMessage("", "");
      render();
    } catch (error) {
      state.entries = [];
      setMessage(error.message || "Could not load schema files", "error");
      render();
    }
  }

  async function loadSelectedSchema() {
    if (!state.selectedFile) {
      setMessage("Select a schema file first.", "warn");
      return;
    }
    try {
      const params = new URLSearchParams();
      params.set("path", state.selectedFile);
      const response = await fetch("/api/schema?" + params.toString());
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load schema");
      }
      state.schema = normalizeSchema(payload.schema || blankSchema);
      state.loadedPath = payload.relative_path || state.selectedFile;
      state.activeIndex = state.schema.fields.length ? 0 : -1;
      state.dirty = false;
      setMessage("Schema loaded.", "ok");
      closeSchemaPicker();
      render();
      focusWorkbench();
    } catch (error) {
      setMessage(error.message || "Could not load schema", "error");
      render();
    }
  }

  function openSchemaPicker(mode) {
    state.pickerMode = mode === "new" || mode === "save" ? mode : "load";
    state.pickerOpen = true;
    state.filter = "";
    if (state.pickerMode === "load") {
      state.selectedFile = "";
    }
    if (!state.entries.length) {
      loadFileList(state.currentPath);
    }
    renderPicker();
  }

  function closeSchemaPicker() {
    state.pickerOpen = false;
    renderPicker();
  }

  function filteredEntries(wantDirectory) {
    return fileBrowser.filteredEntries(state.entries, {
      filter: state.filter,
      wantDirectory: wantDirectory,
    });
  }

  function hasFileEntries() {
    return fileBrowser.hasFileEntries(state.entries);
  }

  function createDraftFromPicker() {
    const path = draftRelativePath();
    if (!path) {
      setMessage("Enter a .json filename for the draft.", "warn");
      return;
    }
    if (/[\\/]/.test(String(state.draftName || ""))) {
      setMessage("Enter only the draft filename; choose the folder in the picker.", "warn");
      return;
    }
    newDraft(path);
    closeSchemaPicker();
    focusWorkbench();
  }

  function openSavePicker() {
    ensureSchemaShape();
    const localError = localSchemaError();
    if (localError) {
      setMessage(localError, "error");
      render();
      return;
    }
    const current = state.loadedPath || "new.schema.json";
    state.draftName = baseName(current) || "new.schema.json";
    state.selectedFile = current;
    const directory = dirName(current);
    state.pickerMode = "save";
    state.pickerOpen = true;
    state.filter = "";
    loadFileList(directory);
    renderPicker();
  }

  function saveSchemaFromPicker() {
    const path = draftRelativePath();
    if (!path) {
      setMessage("Enter a .json filename for the schema.", "warn");
      return;
    }
    if (/[\\/]/.test(String(state.draftName || ""))) {
      setMessage("Enter only the schema filename; choose the folder in the picker.", "warn");
      return;
    }
    saveSchema(path);
  }

  function newDraft(path) {
    state.schema = deepClone(blankSchema);
    state.loadedPath = path;
    state.activeIndex = -1;
    state.dirty = true;
    setMessage("Draft ready. Add a field, then save it to " + path + ".", "info");
    render();
  }

  function addField() {
    ensureSchemaShape();
    const index = state.schema.fields.length + 1;
    state.schema.fields.push({
      name: "new_column_" + index,
      parquet_name: "new_column_" + index,
      type: "string",
      required: false,
    });
    state.activeIndex = state.schema.fields.length - 1;
    state.dirty = true;
    setMessage("", "");
    render();
  }

  function deleteActiveField() {
    ensureSchemaShape();
    if (state.activeIndex < 0 || state.activeIndex >= state.schema.fields.length) {
      return;
    }
    state.schema.fields.splice(state.activeIndex, 1);
    if (state.schema.fields.length === 0) {
      state.activeIndex = -1;
    } else {
      state.activeIndex = Math.min(state.activeIndex, state.schema.fields.length - 1);
    }
    state.dirty = true;
    setMessage("", "");
    render();
  }

  async function saveSchema(path) {
    ensureSchemaShape();
    const localError = localSchemaError();
    if (localError) {
      setMessage(localError, "error");
      render();
      return;
    }
    try {
      const response = await fetch("/api/schema", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          path: path,
          schema: buildSchemaForSave(),
        }),
      });
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not save schema");
      }
      state.schema = normalizeSchema(payload.schema || state.schema);
      state.loadedPath = payload.relative_path || path;
      state.dirty = false;
      closeSchemaPicker();
      await loadFileList(state.currentPath);
      notifySchemaSaved(state.loadedPath);
      setMessage("Schema saved.", "ok");
      render();
      focusWorkbench();
    } catch (error) {
      setMessage(error.message || "Could not save schema", "error");
      render();
    }
  }

  function updateActiveFieldFromForm() {
    ensureSchemaShape();
    if (state.activeIndex < 0 || state.activeIndex >= state.schema.fields.length) {
      return;
    }
    const field = state.schema.fields[state.activeIndex];
    field.name = els.fieldNameInput.value;
    field.parquet_name = els.fieldParquetInput.value;
    field.type = els.fieldTypeInput.value;
    field.required = els.fieldRequiredInput.checked;
    setScalarProperty(field, "default", els.fieldDefaultInput.value, field.type);
    setScalarProperty(field, "override", els.fieldOverrideInput.value, field.type);
    if (field.type === "string") {
      field.lower = els.fieldLowerInput.checked;
      setNumberProperty(field, "min_length", els.fieldMinLengthInput.value);
      setArrayProperty(field, "allowed_values", arrayFromAllowedValueRows());
      setMapProperty(field, "inline_replace", mapFromInlineReplaceRows());
    }
    if (field.type === "int") {
      field.non_zero = els.fieldNonZeroInput.checked;
    }
    if (field.type === "date") {
      setArrayProperty(field, "date_formats", linesFromText(els.fieldDateFormatsInput.value));
    }
    if (field.type === "datetime") {
      setArrayProperty(field, "datetime_formats", linesFromText(els.fieldDatetimeFormatsInput.value));
    }
    state.dirty = true;
    setMessage("", "");
    renderFieldTable();
    renderMeta();
    renderTypeSpecificOptions(field.type);
  }

  function buildSchemaForSave() {
    ensureSchemaShape();
    return {
      fields: state.schema.fields.map(function (field) {
        const out = {};
        stringProperty(out, "name", field.name);
        stringProperty(out, "parquet_name", field.parquet_name);
        stringProperty(out, "type", field.type || "string");
        booleanProperty(out, "required", field.required);
        valueProperty(out, "default", field.default);
        valueProperty(out, "override", field.override);
        if ((field.type || "string") === "string") {
          numberProperty(out, "min_length", field.min_length);
          booleanProperty(out, "lower", field.lower);
          arrayProperty(out, "allowed_values", field.allowed_values);
          mapProperty(out, "inline_replace", field.inline_replace);
        }
        if (field.type === "int") {
          booleanProperty(out, "non_zero", field.non_zero);
        }
        if (field.type === "date") {
          arrayProperty(out, "date_formats", field.date_formats);
        }
        if (field.type === "datetime") {
          arrayProperty(out, "datetime_formats", field.datetime_formats);
        }
        return out;
      }),
    };
  }

  function localSchemaError() {
    ensureSchemaShape();
    if (!state.schema.fields.length) {
      return "Add at least one field before saving.";
    }
    const seen = new Set();
    for (let i = 0; i < state.schema.fields.length; i++) {
      const field = state.schema.fields[i];
      const name = (field.name || "").trim();
      if (!name) {
        return "Field " + (i + 1) + " needs a CSV column name.";
      }
      if (seen.has(name)) {
        return "Duplicate field name: " + name;
      }
      seen.add(name);
      if (supportedTypes.indexOf(field.type || "string") < 0) {
        return "Field " + name + " has an unsupported type.";
      }
    }
    return "";
  }

  function render() {
    renderPicker();
    renderMeta();
    renderFieldTable();
    renderFieldEditor();
    renderMessage();
  }

  function renderPicker() {
    els.pickerModal.hidden = !state.pickerOpen;
    els.pickerFilterInput.value = state.filter;
    els.draftNameInput.value = state.draftName;

    const isNewDraft = state.pickerMode === "new";
    const isSave = state.pickerMode === "save";
    const usesFilename = isNewDraft || isSave;
    els.pickerTitle.textContent = isNewDraft ? "New Schema" : isSave ? "Save Schema" : "Load Schema";
    els.pickerSubtitle.textContent = isNewDraft
      ? "Choose a folder and filename."
      : isSave
        ? "Choose the same filename, or enter a new one to save a copy."
        : "Select an existing schema file.";
    els.draftLocationPanel.hidden = !usesFilename;
    els.schemaFileSelect.disabled = isNewDraft;
    els.schemaNameLabel.textContent = isNewDraft ? "New schema filename" : "Save as";
    els.pickerListLabel.textContent = usesFilename ? "Existing schema files in this folder" : "Schema files";
    els.pickerChooseButton.textContent = isNewDraft ? "Create Schema" : isSave ? "Save Here" : "Load Schema";

    const directories = filteredEntries(true);
    const files = filteredEntries(false);

    els.currentPath.textContent = "/" + (state.currentPath || "");
    els.upButton.disabled = !state.currentPath && !state.parentPath;

    fileBrowser.renderDirectoryList(els.directoryList, directories, {
      onChoose: loadFileList,
    });

    fileBrowser.populateSelect(els.schemaFileSelect, files, {
      selectedValue: state.selectedFile,
      emptyText: hasFileEntries() ? "No files match the current filter" : "No schema JSON files in this directory",
    });

    if (usesFilename) {
      const draftPath = draftRelativePath();
      els.draftPathPreview.textContent = draftPath ? "Target: " + draftPath : "Enter a schema filename.";
      els.pickerSelectionSummary.textContent = draftPath || "No filename set.";
      els.pickerChooseButton.disabled = !draftPath;
      return;
    }

    els.draftPathPreview.textContent = "";
    els.pickerSelectionSummary.textContent = state.selectedFile || "No file selected.";
    els.pickerChooseButton.disabled = !state.selectedFile;
  }

  function renderMeta() {
    const count = state.schema.fields.length;
    els.workbench.classList.toggle("schema-workbench--ready", Boolean(state.loadedPath));
    els.fieldCount.textContent = count + (count === 1 ? " field" : " fields");
    els.addFieldButton.disabled = !state.loadedPath;
    els.saveButton.disabled = !state.schema.fields.length;
  }

  function renderFieldTable() {
    ensureSchemaShape();
    if (!state.schema.fields.length) {
      const message = state.loadedPath ? "Add a field to begin this draft." : "Load or create a schema to begin.";
      els.fieldTableBody.innerHTML = '<tr><td colspan="3">' + escapeHTML(message) + '</td></tr>';
      return;
    }
    els.fieldTableBody.innerHTML = state.schema.fields.map(function (field, index) {
      const active = index === state.activeIndex ? " field-row--active" : "";
      return [
        '<tr class="field-row' + active + '" data-index="' + index + '">',
        "<td><strong>" + escapeHTML(field.name || "Unnamed field") + "</strong><span>" + escapeHTML(field.parquet_name || "") + "</span></td>",
        "<td>" + escapeHTML(field.type || "string") + "</td>",
        "<td>" + (field.required ? "Yes" : "No") + "</td>",
        "</tr>",
      ].join("");
    }).join("");
    els.fieldTableBody.querySelectorAll("[data-index]").forEach(function (row) {
      row.addEventListener("click", function () {
        state.activeIndex = Number(row.getAttribute("data-index"));
        render();
      });
    });
  }

  function renderFieldEditor() {
    ensureSchemaShape();
    const field = state.schema.fields[state.activeIndex];
    const hasField = Boolean(field);
    els.fieldEditor.hidden = !hasField;
    els.fieldEditorEmpty.hidden = hasField;
    els.deleteFieldButton.disabled = !hasField;

    if (!hasField) {
      els.fieldDetailSubtitle.textContent = state.loadedPath ? "Select a column to edit its rules." : "Load or create a schema first.";
      els.fieldEditorEmpty.textContent = state.loadedPath ? "Select a field from the table, or add a new field." : "Load a schema file or create a draft to start editing.";
      return;
    }

    els.fieldDetailSubtitle.textContent = field.name || "Unnamed field";
    els.fieldNameInput.value = field.name || "";
    els.fieldParquetInput.value = field.parquet_name || "";
    els.fieldTypeInput.value = supportedTypes.indexOf(field.type) >= 0 ? field.type : "string";
    els.fieldMinLengthInput.value = field.min_length || "";
    els.fieldDefaultInput.value = valueText(field.default);
    els.fieldOverrideInput.value = valueText(field.override);
    els.fieldRequiredInput.checked = Boolean(field.required);
    els.fieldLowerInput.checked = Boolean(field.lower);
    els.fieldNonZeroInput.checked = Boolean(field.non_zero);
    renderAllowedValueRows(field.allowed_values);
    renderInlineReplaceRows(field.inline_replace);
    els.fieldDateFormatsInput.value = arrayText(field.date_formats);
    els.fieldDatetimeFormatsInput.value = arrayText(field.datetime_formats);
    renderTypeSpecificOptions(els.fieldTypeInput.value);
  }

  function renderTypeSpecificOptions(type) {
    els.typeSpecificOptions.forEach(function (option) {
      const allowedTypes = String(option.getAttribute("data-field-types") || "").split(/\s+/);
      option.hidden = allowedTypes.indexOf(type || "string") < 0;
    });
  }

  function renderAllowedValueRows(values) {
    els.fieldAllowedRows.innerHTML = "";
    const rows = Array.isArray(values) ? values : [];
    rows.forEach(function (value) {
      addAllowedValueRow(value);
    });
  }

  function addAllowedValueRowAndUpdate() {
    const row = addAllowedValueRow("");
    const input = row.querySelector("[data-allowed-value]");
    if (input) {
      input.focus();
    }
    updateActiveFieldFromForm();
  }

  function addAllowedValueRow(value) {
    const row = document.createElement("div");
    row.className = "allowed-value-row";
    row.innerHTML = [
      '<input type="text" autocomplete="off" data-allowed-value placeholder="Value" value="' + escapeHTML(value) + '">',
      '<button class="button button--secondary button--small schema-list-row__remove" type="button" data-allowed-value-remove title="Remove allowed value" aria-label="Remove allowed value">Remove</button>',
    ].join("");
    els.fieldAllowedRows.appendChild(row);
    return row;
  }

  function renderInlineReplaceRows(value) {
    els.fieldInlineReplaceRows.innerHTML = "";
    const keys = value && typeof value === "object" && !Array.isArray(value) ? Object.keys(value).sort() : [];
    keys.forEach(function (key) {
      addInlineReplaceRow(key, value[key]);
    });
  }

  function addInlineReplaceRowAndUpdate() {
    const row = addInlineReplaceRow("", "");
    const input = row.querySelector("[data-inline-replace-from]");
    if (input) {
      input.focus();
    }
    updateActiveFieldFromForm();
  }

  function addInlineReplaceRow(from, to) {
    const row = document.createElement("div");
    row.className = "inline-replace-row";
    row.innerHTML = [
      '<input type="text" autocomplete="off" data-inline-replace-from placeholder="From" value="' + escapeHTML(from) + '">',
      '<input type="text" autocomplete="off" data-inline-replace-to placeholder="To" value="' + escapeHTML(to) + '">',
      '<button class="button button--secondary button--small schema-list-row__remove" type="button" data-inline-replace-remove title="Remove replacement" aria-label="Remove replacement">Remove</button>',
    ].join("");
    els.fieldInlineReplaceRows.appendChild(row);
    return row;
  }

  function renderMessage() {
    els.message.textContent = state.message;
    els.message.className = "form-message schema-message";
    if (state.messageTone) {
      els.message.classList.add("schema-message--" + state.messageTone);
    }
  }

  function normalizeSchema(schema) {
    const normalized = schema && typeof schema === "object" ? deepClone(schema) : deepClone(blankSchema);
    normalized.fields = Array.isArray(normalized.fields) ? normalized.fields : [];
    normalized.fields = normalized.fields.map(function (field) {
      const next = field && typeof field === "object" ? field : {};
      next.type = supportedTypes.indexOf(next.type) >= 0 ? next.type : "string";
      next.allowed_values = Array.isArray(next.allowed_values) ? next.allowed_values : [];
      next.date_formats = Array.isArray(next.date_formats) ? next.date_formats : [];
      next.datetime_formats = Array.isArray(next.datetime_formats) ? next.datetime_formats : [];
      next.inline_replace = next.inline_replace && typeof next.inline_replace === "object" && !Array.isArray(next.inline_replace) ? next.inline_replace : {};
      return next;
    });
    return normalized;
  }

  function ensureSchemaShape() {
    state.schema = normalizeSchema(state.schema);
  }

  function setMessage(message, tone) {
    state.message = message;
    state.messageTone = tone || "";
  }

  async function parseJSON(response) {
    const text = await response.text();
    if (!text) {
      return null;
    }
    try {
      return JSON.parse(text);
    } catch (error) {
      throw new Error("Server returned unreadable JSON");
    }
  }

  function setNumberProperty(target, key, value) {
    const clean = String(value || "").trim();
    if (!clean) {
      delete target[key];
      return;
    }
    const parsed = Number(clean);
    if (Number.isFinite(parsed)) {
      target[key] = Math.max(0, Math.trunc(parsed));
    }
  }

  function setScalarProperty(target, key, value, type) {
    const clean = String(value || "").trim();
    if (!clean) {
      delete target[key];
      return;
    }
    if (type === "int") {
      const parsed = Number(clean);
      target[key] = Number.isFinite(parsed) ? Math.trunc(parsed) : clean;
      return;
    }
    if (type === "float") {
      const parsed = Number(clean);
      target[key] = Number.isFinite(parsed) ? parsed : clean;
      return;
    }
    target[key] = clean;
  }

  function setArrayProperty(target, key, values) {
    if (values.length) {
      target[key] = values;
    } else {
      delete target[key];
    }
  }

  function setMapProperty(target, key, values) {
    if (Object.keys(values).length) {
      target[key] = values;
    } else {
      delete target[key];
    }
  }

  function stringProperty(out, key, value) {
    const clean = String(value || "").trim();
    if (clean) {
      out[key] = clean;
    }
  }

  function booleanProperty(out, key, value) {
    if (value) {
      out[key] = true;
    }
  }

  function numberProperty(out, key, value) {
    if (Number.isFinite(value) && value > 0) {
      out[key] = value;
    }
  }

  function arrayProperty(out, key, value) {
    if (Array.isArray(value) && value.length) {
      out[key] = value;
    }
  }

  function mapProperty(out, key, value) {
    if (value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length) {
      out[key] = value;
    }
  }

  function valueProperty(out, key, value) {
    if (value !== undefined && value !== null && value !== "") {
      out[key] = value;
    }
  }

  function linesFromText(text) {
    return String(text || "")
      .split(/\r?\n/)
      .map(function (line) { return line.trim(); })
      .filter(Boolean);
  }

  function arrayFromAllowedValueRows() {
    const out = [];
    els.fieldAllowedRows.querySelectorAll("[data-allowed-value]").forEach(function (input) {
      const value = input.value.trim();
      if (value) {
        out.push(value);
      }
    });
    return out;
  }

  function mapFromInlineReplaceRows() {
    const out = {};
    els.fieldInlineReplaceRows.querySelectorAll(".inline-replace-row").forEach(function (row) {
      const keyInput = row.querySelector("[data-inline-replace-from]");
      const valueInput = row.querySelector("[data-inline-replace-to]");
      const key = keyInput ? keyInput.value.trim() : "";
      const value = valueInput ? valueInput.value.trim() : "";
      if (key) {
        out[key] = value;
      }
    });
    return out;
  }

  function arrayText(value) {
    return Array.isArray(value) ? value.join("\n") : "";
  }

  function valueText(value) {
    if (value === undefined || value === null) {
      return "";
    }
    return String(value);
  }

  function draftRelativePath() {
    return fileBrowser.relativeDraftPath(state.currentPath, state.draftName, "json");
  }

  function normalizedDraftName() {
    return fileBrowser.normalizedFilename(state.draftName, "json");
  }

  function baseName(path) {
    return fileBrowser.baseName(path);
  }

  function dirName(path) {
    return fileBrowser.dirName(path);
  }

  function cleanRelativePath(path) {
    return fileBrowser.cleanRelativePath(path);
  }

  function notifySchemaSaved(path) {
    const relativePath = cleanRelativePath(path);
    if (!relativePath) {
      return;
    }
    const payload = JSON.stringify({
      path: relativePath,
      saved_at: Date.now(),
    });
    try {
      window.localStorage.setItem(schemaEditorStorageKey, payload);
    } catch (error) {
      return;
    }
    try {
      window.dispatchEvent(new CustomEvent("gvy:schema-saved", { detail: JSON.parse(payload) }));
    } catch (error) {
      return;
    }
  }

  function focusWorkbench() {
    window.setTimeout(function () {
      const target = !els.fieldEditor.hidden ? els.fieldNameInput : els.addFieldButton;
      if (target) {
        target.focus({ preventScroll: false });
        return;
      }
      if (els.workbench) {
        els.workbench.focus({ preventScroll: false });
      }
    }, 0);
  }

  function deepClone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function escapeHTML(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  init();
})();
