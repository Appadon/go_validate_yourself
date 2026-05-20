(function () {
  "use strict";

  const bootstrap = window.GVY_UI_BOOTSTRAP || {};
  const phaseOrder = ["split", "validate", "batch"];

  const state = {
    health: bootstrap.server || { status: "ok", busy: false, working_root: "" },
    runs: [],
    snapshot: null,
    result: null,
    events: [],
    seenEvents: new Set(),
    stream: null,
    refreshTimer: 0,
    errorSummary: null,
    errorExplorerOpen: false,
  };

  const els = {
    serverStatusText: document.getElementById("server-status-text"),
    serverStatusBadge: document.getElementById("server-status-badge"),
    refreshRunsButton: document.getElementById("refresh-runs-button"),
    status: document.getElementById("run-manager-status"),
    runListBody: document.getElementById("run-list-body"),
    runListEmpty: document.getElementById("run-list-empty"),
    modal: document.getElementById("run-detail-modal"),
    backdrop: document.getElementById("run-detail-backdrop"),
    closeButton: document.getElementById("run-detail-close-button"),
    copyErrorSummaryButton: document.getElementById("copy-error-summary-button"),
    downloadJSONButton: document.getElementById("download-run-json-button"),
    openErrorExplorerButton: document.getElementById("open-error-explorer-button"),
    detailTitle: document.getElementById("run-detail-title"),
    detailStatus: document.getElementById("run-detail-status"),
    stageDetail: document.getElementById("stage-detail"),
    phaseHeading: document.getElementById("phase-heading"),
    phaseDetail: document.getElementById("phase-detail"),
    progressFill: document.getElementById("progress-fill"),
    phaseTimeline: document.getElementById("phase-timeline"),
    runIDValue: document.getElementById("run-id-value"),
    runInputFilenameValue: document.getElementById("run-input-filename-value"),
    runWorkspaceValue: document.getElementById("run-workspace-value"),
    runRuntimeValue: document.getElementById("run-runtime-value"),
    performanceCards: document.getElementById("performance-cards"),
    eventLog: document.getElementById("event-log"),
    errorExplorerModal: document.getElementById("error-explorer-modal"),
    errorExplorerBackdrop: document.getElementById("error-explorer-backdrop"),
    errorExplorerCloseButton: document.getElementById("error-explorer-close-button"),
    errorExplorerFrame: document.getElementById("error-explorer-frame"),
  };

  function init() {
    bindEvents();
    render();
    refreshRuns();
  }

  function bindEvents() {
    els.refreshRunsButton.addEventListener("click", refreshRuns);
    els.closeButton.addEventListener("click", closeModal);
    els.backdrop.addEventListener("click", closeModal);
    els.copyErrorSummaryButton.addEventListener("click", copyErrorSummary);
    els.downloadJSONButton.addEventListener("click", downloadJSONReport);
    els.openErrorExplorerButton.addEventListener("click", openErrorExplorer);
    els.errorExplorerCloseButton.addEventListener("click", closeErrorExplorer);
    els.errorExplorerBackdrop.addEventListener("click", closeErrorExplorer);
  }

  async function refreshRuns() {
    window.clearTimeout(state.refreshTimer);
    try {
      const response = await fetch("/api/runs");
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load runs");
      }
      state.runs = Array.isArray(payload.runs) ? payload.runs : [];
      setStatus(state.runs.length ? String(state.runs.length) + " run(s) loaded." : "No runs found.", "info");
      await refreshHealth();
      render();
      if (state.runs.some(function (run) { return !isTerminalState(run.state); })) {
        state.refreshTimer = window.setTimeout(refreshRuns, 2000);
      }
    } catch (error) {
      setStatus(error.message || "Could not load runs.", "error");
      render();
    }
  }

  async function refreshHealth() {
    try {
      const response = await fetch("/health");
      if (response.ok) {
        state.health = await response.json();
      }
    } catch (error) {
      return;
    }
  }

  async function openRun(runId) {
    if (!runId) {
      return;
    }
    closeStream();
    state.snapshot = null;
    state.result = null;
    state.events = [];
    state.seenEvents = new Set();
    state.errorSummary = null;
    els.modal.hidden = false;
    setDetailStatus("Loading run...", "info");
    renderDetail();

    try {
      const response = await fetch("/api/runs/" + encodeURIComponent(runId));
      const payload = await parseJSON(response);
      if (!response.ok) {
        throw new Error(payload && payload.message ? payload.message : "Could not load run");
      }
      replaceSnapshot(payload.run);
      setDetailStatus("", "info");
      renderDetail();
      if (isTerminalState(payload.run.state)) {
        await fetchResult(runId);
      } else {
        openStream(runId);
      }
    } catch (error) {
      setDetailStatus(error.message || "Could not load run.", "error");
    }
  }

  async function fetchResult(runId) {
    try {
      const response = await fetch("/api/runs/" + encodeURIComponent(runId) + "/result");
      const payload = await parseJSON(response);
      if (response.ok) {
        state.result = payload;
        renderDetail();
      }
    } catch (error) {
      return;
    }
  }

  function replaceSnapshot(snapshot) {
    state.snapshot = snapshot || null;
    state.events = [];
    state.seenEvents = new Set();
    (snapshot && snapshot.events ? snapshot.events : []).forEach(pushEvent);
  }

  function pushEvent(event) {
    if (!event) {
      return;
    }
    if (event.type === "telemetry") {
      if (state.snapshot && event.metrics && event.metrics.performance) {
        state.snapshot.performance = event.metrics.performance;
      }
      return;
    }
    const key = [
      event.time || "",
      event.phase || "",
      event.type || "",
      event.message || "",
      event.percent == null ? "" : String(event.percent),
      JSON.stringify(event.metrics || {}),
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
    closeStream();
    const source = new EventSource("/api/runs/" + encodeURIComponent(runId) + "/events");
    state.stream = source;
    source.addEventListener("progress", function (message) {
      try {
        const event = JSON.parse(message.data);
        if (event.type === "telemetry") {
          if (state.snapshot && event.metrics && event.metrics.performance) {
            state.snapshot.performance = event.metrics.performance;
          }
          renderDetail();
          return;
        }
        pushEvent(event);
        if (state.snapshot) {
          state.snapshot.latest_event = event;
          if (event.phase === "run" && event.type === "completed") {
            state.snapshot.state = "completed";
          }
          if (event.phase === "run" && event.type === "failed") {
            state.snapshot.state = "failed";
          }
        }
        renderDetail();
      } catch (error) {
        setDetailStatus("Received an unreadable progress event.", "error");
      }
    });
    source.onerror = function () {
      closeStream();
      if (state.snapshot && !isTerminalState(state.snapshot.state)) {
        window.setTimeout(function () {
          openRun(state.snapshot.run_id);
        }, 1000);
      } else if (state.snapshot) {
        fetchResult(state.snapshot.run_id);
      }
    };
  }

  function closeStream() {
    if (state.stream) {
      state.stream.close();
      state.stream = null;
    }
  }

  async function loadErrorSummary() {
    if (state.errorSummary) {
      return state.errorSummary;
    }
    const path = currentErrorDir();
    if (!path) {
      return null;
    }
    const params = new URLSearchParams();
    params.set("path", path);
    params.set("limit", "20");
    const response = await fetch("/api/errors/report?" + params.toString());
    const payload = await parseJSON(response);
    if (!response.ok) {
      throw new Error(payload && payload.message ? payload.message : "Could not load error summary");
    }
    state.errorSummary = payload;
    return payload;
  }

  async function copyErrorSummary() {
    try {
      const summary = await loadErrorSummary();
      const text = buildErrorSummaryText(summary);
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        copyTextFallback(text);
      }
      setDetailStatus("Error summary copied.", "ok");
    } catch (error) {
      setDetailStatus(error.message || "Could not copy error summary.", "error");
    }
  }

  async function downloadJSONReport() {
    try {
      let summary = state.errorSummary;
      try {
        summary = await loadErrorSummary();
      } catch (error) {
        summary = state.errorSummary;
      }
      const payload = {
        run: state.snapshot || null,
        result: state.result || null,
        error_summary: summary || null,
        events: state.events || [],
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = ((state.snapshot && state.snapshot.run_id) || "gvy-run") + ".report.json";
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
      setDetailStatus("JSON report downloaded.", "ok");
    } catch (error) {
      setDetailStatus(error.message || "Could not download JSON report.", "error");
    }
  }

  function openErrorExplorer() {
    const path = currentErrorDir();
    if (!path) {
      setDetailStatus("No error directory is available for this run.", "warn");
      return;
    }
    const params = new URLSearchParams();
    params.set("embed", "1");
    params.set("path", path);
    state.errorExplorerOpen = true;
    els.errorExplorerFrame.src = "/error-explorer?" + params.toString();
    renderErrorExplorer();
  }

  function closeErrorExplorer() {
    state.errorExplorerOpen = false;
    els.errorExplorerFrame.src = "about:blank";
    renderErrorExplorer();
  }

  function closeModal() {
    closeStream();
    els.modal.hidden = true;
    state.snapshot = null;
    state.result = null;
    state.events = [];
    state.seenEvents = new Set();
    state.errorSummary = null;
    closeErrorExplorer();
    refreshRuns();
  }

  function render() {
    renderServerState();
    renderRunList();
    renderDetail();
    renderErrorExplorer();
  }

  function renderServerState() {
    const busy = Boolean(state.health && state.health.busy);
    els.serverStatusText.textContent = busy ? "Busy" : "Idle";
    els.serverStatusBadge.className = "badge badge--" + (busy ? "warn" : "ok");
    els.serverStatusBadge.textContent = busy ? "Single run occupied" : "Ready";
  }

  function renderRunList() {
    if (!state.runs.length) {
      els.runListBody.innerHTML = "";
      els.runListEmpty.hidden = false;
      return;
    }
    els.runListEmpty.hidden = true;
    els.runListBody.innerHTML = state.runs.map(function (run) {
      const latest = run.latest_event || newestRunEvent(run);
      const workspace = run.workspace || {};
      return [
        '<tr class="run-table-row" data-run-id="' + escapeHTML(run.run_id || "") + '">',
        "<td><code>" + escapeHTML(run.run_id || "Unknown") + "</code></td>",
        '<td><span class="badge badge--' + toneForState(run.state) + '">' + escapeHTML(formatState(run.state)) + "</span></td>",
        "<td>" + escapeHTML(formatTimestamp(run.created_at)) + "</td>",
        "<td>" + escapeHTML(formatTimestamp(run.started_at)) + "</td>",
        "<td>" + escapeHTML(formatTimestamp(run.finished_at)) + "</td>",
        "<td>" + escapeHTML(inputFilenameText(run)) + "</td>",
        "<td>" + escapeHTML(formatPhase(dataPhaseForEvent(latest) || "")) + "</td>",
        "<td>" + escapeHTML(toRelativePath(workspace.root_dir || "")) + "</td>",
        "</tr>",
      ].join("");
    }).join("");
    els.runListBody.querySelectorAll("[data-run-id]").forEach(function (row) {
      row.addEventListener("click", function () {
        openRun(row.getAttribute("data-run-id") || "");
      });
    });
  }

  function renderDetail() {
    const snapshot = state.snapshot;
    if (!snapshot) {
      els.detailTitle.textContent = "Run report";
      els.stageDetail.textContent = "Preparing";
      els.phaseHeading.textContent = "Ready";
      els.phaseDetail.textContent = "No run selected.";
      els.progressFill.style.width = "0%";
      els.runIDValue.textContent = "No run selected";
      els.runInputFilenameValue.textContent = "Not selected";
      els.runWorkspaceValue.textContent = "Not available";
      els.runRuntimeValue.textContent = "Not started";
      els.phaseTimeline.innerHTML = "";
      els.performanceCards.innerHTML = "";
      renderEvents();
      return;
    }

    const latest = newestEvent() || snapshot.latest_event || null;
    const stage = getStageInfo(snapshot, latest);
    const percent = getProgressPercent(snapshot, latest);
    els.detailTitle.textContent = snapshot.run_id || "Run report";
    els.stageDetail.textContent = stage.label;
    els.phaseHeading.textContent = formatState(snapshot.state);
    els.phaseDetail.textContent = phaseSummaryText(snapshot, stage);
    els.progressFill.style.width = (percent == null ? 0 : percent) + "%";
    els.runIDValue.textContent = snapshot.run_id || "Unknown";
    els.runInputFilenameValue.textContent = inputFilenameText(snapshot);
    els.runWorkspaceValue.textContent = toRelativePath(snapshot.workspace && snapshot.workspace.root_dir ? snapshot.workspace.root_dir : "") || "Not available";
    els.runRuntimeValue.textContent = runTimeText(snapshot);
    renderTimeline(snapshot);
    renderPerformance(snapshot);
    renderEvents();
  }

  function renderTimeline(snapshot) {
    const phases = timelinePhases(snapshot);
    const active = activeTimelinePhase(phases);
    const activeIndex = active ? phases.indexOf(active) : -1;
    const completed = snapshot.state === "completed";
    const failed = snapshot.state === "failed";
    els.phaseTimeline.innerHTML = phases.map(function (phase, index) {
      const status = phaseTimelineStatus(index, activeIndex, completed, failed);
      return [
        '<li class="phase-step phase-step--' + status.tone + '">',
        '<span class="phase-step__marker">' + String(index + 1) + '</span>',
        '<span class="phase-step__body">',
        "<strong>" + escapeHTML(formatPhase(phase)) + "</strong>",
        "<span>" + escapeHTML(status.label) + "</span>",
        "</span>",
        "</li>",
      ].join("");
    }).join("");
  }

  function renderPerformance(snapshot) {
    const summary = snapshot.performance_summary || {};
    const live = snapshot.performance || {};
    const cards = [
      ["Peak CPU", percentText(summary.max_cpu_percent || live.cpu_percent)],
      ["Peak memory", bytesText(summary.max_rss_bytes || summary.max_alloc_bytes || (live.memory && live.memory.rss_bytes))],
      ["Peak IO read", rateBytesText(summary.max_io_read_bytes_per_second)],
      ["Peak IO write", rateBytesText(summary.max_io_write_bytes_per_second)],
      ["Input size", bytesText(summary.input_file_bytes)],
      ["Run size", bytesText(summary.run_bytes)],
    ];
    els.performanceCards.innerHTML = cards.map(function (card) {
      return '<div class="fact"><dt>' + escapeHTML(card[0]) + '</dt><dd>' + escapeHTML(card[1]) + "</dd></div>";
    }).join("");
  }

  function renderEvents() {
    if (!state.events.length) {
      els.eventLog.innerHTML = '<li class="event-log__empty">Progress events will appear here once a validation starts.</li>';
      return;
    }
    els.eventLog.innerHTML = state.events.slice().reverse().map(function (event) {
      const meta = [formatTimestamp(event.time), event.type || ""].filter(Boolean);
      if (event.percent != null) {
        meta.push(Math.round(Number(event.percent)) + "%");
      }
      return [
        '<li class="event-item">',
        '<div class="event-head">',
        '<span class="event-phase">' + escapeHTML(formatPhase(event.phase)) + "</span>",
        '<span class="badge badge--' + toneForEvent(event.type) + '">' + escapeHTML(event.type || "update") + "</span>",
        "</div>",
        '<p class="event-message">' + escapeHTML(event.message || defaultEventMessage(event)) + "</p>",
        '<p class="event-meta">' + escapeHTML(meta.join(" . ")) + "</p>",
        "</li>",
      ].join("");
    }).join("");
  }

  function renderErrorExplorer() {
    els.errorExplorerModal.hidden = !state.errorExplorerOpen;
  }

  function setStatus(message, tone) {
    els.status.textContent = message || "";
    els.status.dataset.tone = tone || "info";
  }

  function setDetailStatus(message, tone) {
    els.detailStatus.textContent = message || "";
    els.detailStatus.dataset.tone = tone || "info";
  }

  function currentErrorDir() {
    const workspace = state.snapshot && state.snapshot.workspace ? state.snapshot.workspace : {};
    return toRelativePath(workspace.error_dir || "");
  }

  function buildErrorSummaryText(data) {
    if (!data || !Array.isArray(data.issues)) {
      return "Validation error summary: Not loaded";
    }
    if (!data.issues.length) {
      return "Validation error summary: No error patterns found";
    }
    const lines = ["Validation error summary:"];
    data.issues.slice(0, 10).forEach(function (issue, index) {
      lines.push("");
      lines.push(String(index + 1) + ". " + valueText(issue.field) + " - " + valueText(issue.message) + " (" + valueText(issue.count) + " rows)");
      (issue.samples || []).slice(0, 3).forEach(function (sample) {
        lines.push("   - row " + valueText(sample.row_number) + ": " + valueText(sample.errors));
      });
    });
    return lines.join("\n");
  }

  function getProgressPercent(snapshot, latestEvent) {
    if (latestEvent && latestEvent.percent != null && !Number.isNaN(Number(latestEvent.percent))) {
      return Math.max(0, Math.min(100, Math.round(Number(latestEvent.percent))));
    }
    return snapshot && snapshot.state === "completed" ? 100 : null;
  }

  function getStageInfo(snapshot, latestEvent) {
    if (snapshot.state === "completed") {
      const completedPhase = latestDataPhase();
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

  function phaseSummaryText(snapshot, stage) {
    if (snapshot.state === "completed") {
      return "All selected stages completed.";
    }
    if (snapshot.state === "failed") {
      return "Run failed. The report remains inspectable.";
    }
    if (snapshot.state === "queued") {
      return "Run is waiting to start.";
    }
    switch (stage && stage.phase) {
      case "split":
        return "Splitting the input dataset.";
      case "validate":
        return "Validating records against the schema.";
      case "batch":
        return "Building batch parquet exports.";
      default:
        return "Run is in progress.";
    }
  }

  function timelinePhases(snapshot) {
    const eventPhases = [];
    state.events.forEach(function (event) {
      const phase = dataPhaseForEvent(event);
      if (phase && eventPhases.indexOf(phase) < 0) {
        eventPhases.push(phase);
      }
    });
    if (eventPhases.length) {
      return phaseOrder.filter(function (phase) { return eventPhases.indexOf(phase) >= 0; });
    }
    return snapshot ? phaseOrder.slice() : [];
  }

  function activeTimelinePhase(phases) {
    const phase = latestDataPhase();
    return phase && phases.indexOf(phase) >= 0 ? phase : "";
  }

  function phaseTimelineStatus(index, activeIndex, completed, failed) {
    if (completed) {
      return { tone: "done", label: "Complete" };
    }
    if (failed && (index === activeIndex || activeIndex < 0)) {
      return { tone: index === Math.max(activeIndex, 0) ? "failed" : "waiting", label: index === Math.max(activeIndex, 0) ? "Needs attention" : "Waiting" };
    }
    if (activeIndex < 0) {
      return index === 0 ? { tone: "current", label: "Waiting to start" } : { tone: "waiting", label: "Waiting" };
    }
    if (index < activeIndex) {
      return { tone: "done", label: "Complete" };
    }
    if (index === activeIndex) {
      return { tone: "current", label: "In progress" };
    }
    return { tone: "waiting", label: "Waiting" };
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

  function newestEvent() {
    return state.events.length ? state.events[state.events.length - 1] : null;
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

  function newestRunEvent(run) {
    const events = run && Array.isArray(run.events) ? run.events : [];
    return events.length ? events[events.length - 1] : null;
  }

  function isDataPhase(phase) {
    return phase === "split" || phase === "validate" || phase === "batch";
  }

  function isTerminalState(value) {
    return value === "completed" || value === "failed";
  }

  function inputFilenameText(snapshot) {
    const workspace = snapshot && snapshot.workspace ? snapshot.workspace : {};
    const path = toRelativePath(workspace.input_csv_path || "");
    return path ? displayFileName(path) : "Not selected";
  }

  function runTimeText(snapshot) {
    const seconds = runDurationSeconds(snapshot);
    return seconds == null ? "Not started" : formatDurationShort(seconds);
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

  function toRelativePath(value) {
    const workingRoot = state.health && state.health.working_root ? String(state.health.working_root).replace(/\\/g, "/").replace(/\/+$/, "") : "";
    const normalized = String(value || "").replace(/\\/g, "/");
    if (workingRoot && normalized.indexOf(workingRoot + "/") === 0) {
      return normalized.slice(workingRoot.length + 1);
    }
    return normalized;
  }

  function displayFileName(path) {
    const clean = String(path || "").replace(/\\/g, "/").replace(/\/+$/, "");
    const index = clean.lastIndexOf("/");
    return index >= 0 ? clean.slice(index + 1) : clean;
  }

  function formatState(value) {
    const text = String(value || "unknown");
    return text ? text.charAt(0).toUpperCase() + text.slice(1) : "Unknown";
  }

  function formatPhase(value) {
    const text = String(value || "").trim();
    if (!text) {
      return "Not started";
    }
    return text.charAt(0).toUpperCase() + text.slice(1);
  }

  function formatTimestamp(value) {
    if (!value) {
      return "Not available";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return "Not available";
    }
    return date.toLocaleString();
  }

  function formatDurationShort(seconds) {
    if (seconds < 60) {
      return Math.round(seconds) + "s";
    }
    if (seconds < 3600) {
      return Math.floor(seconds / 60) + "m " + Math.round(seconds % 60) + "s";
    }
    return Math.floor(seconds / 3600) + "h " + Math.floor((seconds % 3600) / 60) + "m";
  }

  function bytesText(value) {
    const number = Number(value || 0);
    if (!Number.isFinite(number) || number <= 0) {
      return "Not available";
    }
    const units = ["B", "KB", "MB", "GB", "TB"];
    let current = number;
    let index = 0;
    while (current >= 1024 && index < units.length - 1) {
      current = current / 1024;
      index += 1;
    }
    return (current >= 10 || index === 0 ? current.toFixed(0) : current.toFixed(1)) + " " + units[index];
  }

  function percentText(value) {
    const number = Number(value);
    return Number.isFinite(number) && number > 0 ? number.toFixed(number >= 10 ? 0 : 1) + "%" : "Not available";
  }

  function rateBytesText(value) {
    const text = bytesText(value);
    return text === "Not available" ? text : text + "/s";
  }

  function valueText(value) {
    if (value == null || value === "") {
      return "Not available";
    }
    return String(value);
  }

  function defaultEventMessage(event) {
    return event && event.type ? formatState(event.type) : "Progress update";
  }

  function toneForState(stateValue) {
    switch (stateValue) {
      case "completed":
        return "ok";
      case "failed":
        return "error";
      case "running":
      case "queued":
        return "warn";
      default:
        return "muted";
    }
  }

  function toneForEvent(type) {
    switch (type) {
      case "completed":
        return "ok";
      case "failed":
        return "error";
      case "started":
      case "progress":
        return "warn";
      default:
        return "muted";
    }
  }

  async function parseJSON(response) {
    try {
      return await response.json();
    } catch (error) {
      return null;
    }
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
