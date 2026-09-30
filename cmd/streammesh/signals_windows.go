//go:build windows

package main

import "os"

// reloadSignals returns the signals that trigger a rule reload on this platform.
// Windows has no SIGHUP, so rules are reloaded through PUT /rules instead.
func reloadSignals() []os.Signal { return nil }

// isReloadSignal always reports false on Windows.
func isReloadSignal(os.Signal) bool { return false }
