/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manager

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
)

func privateMemoryLimitFile(directory string) string {
	name := "memory.limit_in_bytes"
	if common.CheckCgroup2UnifiedMode() {
		name = "memory.max"
	}
	return filepath.Join(directory, name)
}

func privateCommand(t *testing.T, name, body string) string {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	return path
}

func TestMemoryLimitDiagnosticRetainsRealShellError(t *testing.T) {
	// A missing private directory gives a real shell redirection failure without
	// creating or writing any kernel cgroup or changing global host settings.
	path := filepath.Join(t.TempDir(), "missing")
	err := SetExtraCGMemLimitWithTimeoutAndAbsCGPath(5, path, 4096)
	if err == nil || !strings.Contains(err.Error(), "stderr:") || !strings.Contains(err.Error(), "No such file or directory") || !strings.Contains(err.Error(), path) {
		t.Fatalf("real write stderr lost: %v", err)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 || len(exitErr.Stderr) == 0 || len(exitErr.Stderr) > 4096 {
		t.Fatalf("exit status or bounded stderr no longer unwraps: %v", err)
	}
}

func TestMemoryLimitDiagnosticTruncatesRealProcessStderr(t *testing.T) {
	// Bash still performs the real redirection; this controlled child emits more
	// than 4KiB to stderr and exits with a distinctive nonzero status.
	privateCommand(t, "timeout", `i=0; while [ "$i" -lt 5000 ]; do printf x >&2; i=$((i+1)); done; exit 23`)
	directory := t.TempDir()
	err := SetExtraCGMemLimitWithTimeoutAndAbsCGPath(5, directory, 8192)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("original process exit status lost: %v", err)
	}
	if len(exitErr.Stderr) != 4096 || string(exitErr.Stderr) != strings.Repeat("x", 4096) {
		t.Fatalf("stderr was not bounded at 4096 bytes: %d", len(exitErr.Stderr))
	}
	if !strings.HasSuffix(err.Error(), "stderr: "+strings.Repeat("x", 4096)) {
		t.Fatalf("bounded process stderr not visible: %v", err)
	}
	bytes, readErr := os.ReadFile(privateMemoryLimitFile(directory))
	if readErr != nil || len(bytes) != 0 {
		t.Fatalf("failed child unexpectedly wrote a limit: %q %v", bytes, readErr)
	}
}

func TestMemoryLimitDiagnosticPreservesErrorWithoutStderr(t *testing.T) {
	privateCommand(t, "timeout", "exit 17")
	err := SetExtraCGMemLimitWithTimeoutAndAbsCGPath(5, t.TempDir(), 4096)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 17 || err.Error() != "exit status 17" {
		t.Fatalf("empty-stderr error behavior changed: %v", err)
	}
}

func TestMemoryLimitDiagnosticPreservesSuccessfulWrite(t *testing.T) {
	directory := t.TempDir()
	if err := SetExtraCGMemLimitWithTimeoutAndAbsCGPath(5, directory, 4096); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(privateMemoryLimitFile(directory))
	if err != nil || string(bytes) != "4096\n" {
		t.Fatalf("successful write changed: %q %v", bytes, err)
	}
}

func TestMemoryLimitDiagnosticPreservesLegacyTimeoutPolicy(t *testing.T) {
	// Real GNU timeout executes this private echo and kills its sleep process.
	// Existing policy treats an error at/after the configured deadline as nil;
	// diagnostic enrichment must not turn this into a different policy outcome.
	timeoutPath, err := exec.LookPath("timeout")
	if err != nil {
		t.Fatal(err)
	}
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	privateCommand(t, "echo", "printf timeout-child-started >&2; exec "+sleepPath+" 5")
	resolved, err := exec.LookPath("timeout")
	if err != nil || resolved != timeoutPath {
		t.Fatalf("real timeout was replaced: %s %v", resolved, err)
	}
	directory := t.TempDir()
	start := time.Now()
	err = SetExtraCGMemLimitWithTimeoutAndAbsCGPath(1, directory, 4096)
	elapsed := time.Since(start)
	if err != nil || elapsed < time.Second || elapsed >= 4*time.Second {
		t.Fatalf("legacy timeout policy changed: err=%v elapsed=%v", err, elapsed)
	}
	bytes, readErr := os.ReadFile(privateMemoryLimitFile(directory))
	if readErr != nil || len(bytes) != 0 {
		t.Fatalf("timed-out child unexpectedly wrote a limit: %q %v", bytes, readErr)
	}
}
