(function () {
  "use strict";

  function normalizeFilter(value) {
    return String(value || "").trim().toLowerCase();
  }

  function filteredEntries(entries, options) {
    const config = options || {};
    const filter = normalizeFilter(config.filter);
    const wantDirectory = Boolean(config.wantDirectory);
    const exclude = typeof config.exclude === "function" ? config.exclude : null;
    return (entries || []).filter(function (entry) {
      if (!entry || Boolean(entry.is_dir) !== wantDirectory) {
        return false;
      }
      if (exclude && exclude(entry)) {
        return false;
      }
      if (!filter) {
        return true;
      }
      return String(entry.relative_path || entry.name || "").toLowerCase().indexOf(filter) >= 0;
    });
  }

  function hasFileEntries(entries, options) {
    const config = options || {};
    const exclude = typeof config.exclude === "function" ? config.exclude : null;
    return (entries || []).some(function (entry) {
      return entry && !entry.is_dir && !(exclude && exclude(entry));
    });
  }

  function renderDirectoryList(container, directories, options) {
    const config = options || {};
    const emptyText = config.emptyText || "No subdirectories here.";
    const onChoose = typeof config.onChoose === "function" ? config.onChoose : function () {};
    if (!directories.length) {
      container.innerHTML = '<span class="directory-empty">' + escapeHTML(emptyText) + "</span>";
      return;
    }
    container.innerHTML = directories.map(function (entry) {
      const kindAttr = config.kind ? ' data-kind="' + escapeHTML(config.kind) + '"' : "";
      return '<button class="directory-chip" type="button"' + kindAttr + ' data-path="' + escapeHTML(entry.relative_path) + '">/' + escapeHTML(entry.name) + "</button>";
    }).join("");
    container.querySelectorAll("[data-path]").forEach(function (button) {
      button.addEventListener("click", function () {
        onChoose(button.getAttribute("data-path") || "");
      });
    });
  }

  function populateSelect(select, entries, options) {
    const config = options || {};
    const valueFor = typeof config.valueFor === "function" ? config.valueFor : function (entry) { return entry.relative_path; };
    const textFor = typeof config.textFor === "function" ? config.textFor : function (entry) { return entry.name; };
    select.innerHTML = "";
    if (!entries.length) {
      const option = document.createElement("option");
      option.disabled = true;
      option.textContent = config.emptyText || "No files here";
      select.appendChild(option);
      return;
    }
    entries.forEach(function (entry) {
      const option = document.createElement("option");
      option.value = valueFor(entry);
      option.textContent = textFor(entry);
      option.selected = option.value === (config.selectedValue || "");
      select.appendChild(option);
    });
    if (!config.selectedValue && config.clearWhenEmptySelection) {
      select.selectedIndex = -1;
    }
  }

  function normalizedFilename(value, extension) {
    const clean = String(value || "").trim();
    const suffix = "." + String(extension || "json").replace(/^\./, "");
    if (!clean || /[\\/]/.test(clean)) {
      return "";
    }
    return clean.toLowerCase().endsWith(suffix.toLowerCase()) ? clean : clean + suffix;
  }

  function relativeDraftPath(currentPath, filename, extension) {
    const name = normalizedFilename(filename, extension);
    if (!name) {
      return "";
    }
    return currentPath ? String(currentPath).replace(/\/+$/, "") + "/" + name : name;
  }

  function baseName(path) {
    const clean = String(path || "").replace(/\/+$/, "");
    const index = clean.lastIndexOf("/");
    return index >= 0 ? clean.slice(index + 1) : clean;
  }

  function dirName(path) {
    const clean = String(path || "").replace(/\/+$/, "");
    const index = clean.lastIndexOf("/");
    return index >= 0 ? clean.slice(0, index) : "";
  }

  function cleanRelativePath(path) {
    return String(path || "")
      .replace(/\\/g, "/")
      .replace(/^\/+/, "")
      .replace(/\/{2,}/g, "/")
      .trim();
  }

  function escapeHTML(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  window.GVYFileBrowser = {
    baseName: baseName,
    cleanRelativePath: cleanRelativePath,
    dirName: dirName,
    escapeHTML: escapeHTML,
    filteredEntries: filteredEntries,
    hasFileEntries: hasFileEntries,
    normalizeFilter: normalizeFilter,
    normalizedFilename: normalizedFilename,
    populateSelect: populateSelect,
    relativeDraftPath: relativeDraftPath,
    renderDirectoryList: renderDirectoryList,
  };
})();
