package main

import (
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

// isTmpDataDir reports whether dir is /tmp or lives under it.
func isTmpDataDir(dir string) bool {
	c := filepath.Clean(dir)
	return c == "/tmp" || strings.HasPrefix(c, "/tmp/")
}

// warnIfTmpDataDir logs a one-time warning when --data-dir is under /tmp
// (the default). It never rejects the configuration.
func warnIfTmpDataDir(logger *zap.Logger, dir string) {
	if isTmpDataDir(dir) {
		logger.Warn("--data-dir is under /tmp: data is lost on reboot and the directory may be readable by other users; set --data-dir explicitly for production (e.g. /var/lib/sentinel)",
			zap.String("data_dir", dir))
	}
}
