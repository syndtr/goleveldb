package leveldb

import (
	"net/http"
	"os"
	"os/exec"
	"testing"
)

const testServerPort = ":9714"

// TestInBrowser is a harness that allows us to use `go test` in order to run
// WebAssembly tests in a headless browser.
func TestInBrowser(t *testing.T) {
	// Install dependencies with npm
	cmd := exec.Command("npm", "install", "--no-save")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Log(string(output))
		t.Fatal(err)
	}

	cmd = exec.Command("go", "build", "-o", "main.wasm", ".")
	cmd.Env = append(os.Environ(), []string{"GOOS=js", "GOARCH=wasm"}...)
	cmd.Dir = "./browser-tests"
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Log(string(output))
		t.Fatal(err)
	}

	go func() {
		if err := http.ListenAndServe(testServerPort, http.FileServer(http.Dir("./browser-tests"))); err != nil {
			t.Fatal(err)
		}
	}()

	cmd = exec.Command("node", "./node_modules/.bin/qunit-puppeteer", "http://localhost"+testServerPort)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Log(string(output))
		t.Fatal(err)
	}
}
