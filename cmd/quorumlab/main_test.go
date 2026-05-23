package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDemoAndEvaluateExportActualJSON(t *testing.T) {
	for _, command := range []string{"demo", "evaluate"} {
		path := filepath.Join(t.TempDir(), command+".json")
		if code := run([]string{command, "--output", path}); code != 0 {
			t.Fatal(command, code)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var report map[string]any
		if json.Unmarshal(raw, &report) != nil || len(report) == 0 {
			t.Fatal("invalid output")
		}
	}
}
func TestCLIRejectsBadCommandsAndExportFailures(t *testing.T) {
	if run([]string{"shell"}) != 2 || run([]string{"demo", "extra"}) != 2 {
		t.Fatal("invalid CLI accepted")
	}
	if code := run([]string{"demo", "--output", t.TempDir()}); code != 2 {
		t.Fatal("write failure", code)
	}
}
