package schema

import "embed"

// FS holds the proto sources the collector and frontend generate their clients from.
//
//go:embed querysheriff
var FS embed.FS
