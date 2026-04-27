package web

import "embed"

// Files contains the embedded Stage 5 UI templates and static assets.
//
//go:embed templates/* static/css/* static/js/*
var Files embed.FS
