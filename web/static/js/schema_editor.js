(function () {
  "use strict";

  const blankSchema = { fields: [] };
  const supportedTypes = ["string", "int", "float", "date", "datetime"];

  const state = {
    currentPath: "",
    parentPath: "",
    entries: [],
    selectedFile: "",
    loadedPath: "",
    schema: deepClone(blankSchema),
    activeIndex: -1,
    dirty: false,
    message: "",
    messageTone: "",
  };

  const els = {
    refreshButton: document.getElementById("refresh-button"),
    currentPath: document.getElementById("current-path"),
    upButton: document.getElementById("up-button"),
    directoryList: document.getElementById("directory-list"),
    schemaFileSelect: document.getElementById("schema-file-select"),
    loadButton: document.getElementById("load-button"),
    newButton: document.getElementById("new-button"),
    loadedPath: document.getElementById("loaded-path"),
    savePathInput: document.getElementById("save-path-input"),
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
    fieldExcludeMissingInput: document.getElementById("field-exclude-missing-input"),
    fieldLowerInput: document.getElementById("field-lower-input"),
    fieldNonZeroInput: document.getElementById("field-non-zero-input"),
    fieldAllowedInput: document.getElementById("field-allowed-input"),
    fieldInlineReplaceInput: document.getElementById("field-inline-replace-input"),
    fieldDateFormatsInput: document.getElementById("field-date-formats-input"),
    fieldDatetimeFormatsInput: document.getElementById("field-datetime-formats-input"),
  };

  function init() {
    bindEvents();
    render();
    loadFileList("");
  }

  function bindEvents() {
    els.refreshButton.addEventListener("click", function () {
      loadFileList(state.currentPath);
    });
    els.upButton.addEventListener("click", function () {
      loadFileList(state.parentPath || "");
    });
    els.schemaFileSelect.addEventListener("change", function () {
      state.selectedFile = els.schemaFileSelect.value || "";
      render();
    });
    els.schemaFileSelect.addEventListener("dblclick", loadSelectedSchema);
    els.loadButton.addEventListener("click", loadSelectedSchema);
    els.newButton.addEventListener("click", newDraft);
    els.saveButton.addEventListener("click", saveSchema);
    els.addFieldButton.addEventListener("click", addField);
    els.deleteFieldButton.addEventListener("click", deleteActiveField);

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
      els.fieldExcludeMissingInput,
      els.fieldLowerInput,
      els.fieldNonZeroInput,
      els.fieldAllowedInput,
      els.fieldInlineReplaceInput,
      els.fieldDateFormatsInput,
      els.fieldDatetimeFormatsInput,
    ];
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
      els.savePathInput.value = state.loadedPath;
      setMessage("Schema loaded.", "ok");
      render();
    } catch (error) {
      setMessage(error.message || "Could not load schema", "error");
      render();
    }
  }

  function newDraft() {
    state.schema = deepClone(blankSchema);
    state.loadedPath = "";
    state.activeIndex = -1;
    state.dirty = false;
    els.savePathInput.value = "schemas/new.schema.json";
    setMessage("New empty schema draft created.", "info");
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

  async function saveSchema() {
    ensureSchemaShape();
    const path = els.savePathInput.value.trim();
    if (!path) {
      setMessage("Enter a .json save path.", "warn");
      return;
    }
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
      els.savePathInput.value = state.loadedPath;
      setMessage("Schema saved.", "ok");
      await loadFileList(state.currentPath);
      render();
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
    field.exclude_if_missing = els.fieldExcludeMissingInput.checked;
    field.lower = els.fieldLowerInput.checked;
    field.non_zero = els.fieldNonZeroInput.checked;
    setNumberProperty(field, "min_length", els.fieldMinLengthInput.value);
    setScalarProperty(field, "default", els.fieldDefaultInput.value, field.type);
    setScalarProperty(field, "override", els.fieldOverrideInput.value, field.type);
    setArrayProperty(field, "allowed_values", linesFromText(els.fieldAllowedInput.value));
    setMapProperty(field, "inline_replace", mapFromText(els.fieldInlineReplaceInput.value));
    setArrayProperty(field, "date_formats", linesFromText(els.fieldDateFormatsInput.value));
    setArrayProperty(field, "datetime_formats", linesFromText(els.fieldDatetimeFormatsInput.value));
    state.dirty = true;
    setMessage("", "");
    renderFieldTable();
    renderMeta();
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
        booleanProperty(out, "exclude_if_missing", field.exclude_if_missing);
        numberProperty(out, "min_length", field.min_length);
        booleanProperty(out, "lower", field.lower);
        arrayProperty(out, "allowed_values", field.allowed_values);
        mapProperty(out, "inline_replace", field.inline_replace);
        valueProperty(out, "default", field.default);
        valueProperty(out, "override", field.override);
        booleanProperty(out, "non_zero", field.non_zero);
        arrayProperty(out, "date_formats", field.date_formats);
        arrayProperty(out, "datetime_formats", field.datetime_formats);
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
    renderBrowser();
    renderMeta();
    renderFieldTable();
    renderFieldEditor();
    renderMessage();
  }

  function renderBrowser() {
    const directories = state.entries.filter(function (entry) { return entry.is_dir; });
    const files = state.entries.filter(function (entry) { return !entry.is_dir; });

    els.currentPath.textContent = "/" + (state.currentPath || "");
    els.upButton.disabled = !state.currentPath && !state.parentPath;

    if (!directories.length) {
      els.directoryList.innerHTML = '<span class="directory-empty">No subdirectories here.</span>';
    } else {
      els.directoryList.innerHTML = directories.map(function (entry) {
        return '<button class="directory-chip" type="button" data-path="' + escapeHTML(entry.relative_path) + '">/' + escapeHTML(entry.name) + '</button>';
      }).join("");
      els.directoryList.querySelectorAll("[data-path]").forEach(function (button) {
        button.addEventListener("click", function () {
          loadFileList(button.getAttribute("data-path") || "");
        });
      });
    }

    els.schemaFileSelect.innerHTML = "";
    if (!files.length) {
      const option = document.createElement("option");
      option.disabled = true;
      option.textContent = "No schema JSON files in this directory";
      els.schemaFileSelect.appendChild(option);
    } else {
      files.forEach(function (entry) {
        const option = document.createElement("option");
        option.value = entry.relative_path;
        option.textContent = entry.name;
        option.selected = entry.relative_path === state.selectedFile;
        els.schemaFileSelect.appendChild(option);
      });
    }

    els.loadButton.disabled = !state.selectedFile;
  }

  function renderMeta() {
    const count = state.schema.fields.length;
    els.loadedPath.textContent = state.loadedPath ? "Loaded: " + state.loadedPath + (state.dirty ? " (unsaved)" : "") : "No schema loaded.";
    els.fieldCount.textContent = count + (count === 1 ? " field" : " fields");
    els.saveButton.disabled = !state.schema.fields.length;
  }

  function renderFieldTable() {
    ensureSchemaShape();
    if (!state.schema.fields.length) {
      els.fieldTableBody.innerHTML = '<tr><td colspan="3">Load or create a schema to begin.</td></tr>';
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
      els.fieldDetailSubtitle.textContent = "Select a column to edit its rules.";
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
    els.fieldExcludeMissingInput.checked = Boolean(field.exclude_if_missing);
    els.fieldLowerInput.checked = Boolean(field.lower);
    els.fieldNonZeroInput.checked = Boolean(field.non_zero);
    els.fieldAllowedInput.value = arrayText(field.allowed_values);
    els.fieldInlineReplaceInput.value = mapText(field.inline_replace);
    els.fieldDateFormatsInput.value = arrayText(field.date_formats);
    els.fieldDatetimeFormatsInput.value = arrayText(field.datetime_formats);
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

  function mapFromText(text) {
    const out = {};
    linesFromText(text).forEach(function (line) {
      const separator = line.indexOf("=>") >= 0 ? "=>" : "=";
      const index = line.indexOf(separator);
      if (index < 0) {
        return;
      }
      const key = line.slice(0, index).trim();
      const value = line.slice(index + separator.length).trim();
      if (key) {
        out[key] = value;
      }
    });
    return out;
  }

  function arrayText(value) {
    return Array.isArray(value) ? value.join("\n") : "";
  }

  function mapText(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return "";
    }
    return Object.keys(value).sort().map(function (key) {
      return key + " => " + value[key];
    }).join("\n");
  }

  function valueText(value) {
    if (value === undefined || value === null) {
      return "";
    }
    return String(value);
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
