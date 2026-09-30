//go:build !windows

package main

import (
	"os"
	"syscall"
)

// reloadSignals returns the signals that trigger a rule reload on this platform.
func reloadSignals() []os.Signal { return []os.Signal{syscall.SIGHUP} }

// isReloadSignal reports whether a received signal means "reload the rules".
func isReloadSignal(sig os.Signal) bool { return sig == syscall.SIGHUP }
