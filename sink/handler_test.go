package sink

import (
	"os"
	"path/filepath"
	"testing"

	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap/zaptest"
)

func TestCursorSaveAndLoad(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "test.cursor")

	handler := &Handler{
		cursorFilePath: cursorFilePath,
		logger:         logger,
	}

	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="

	originalCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor from test string: %v", err)
	}

	err = handler.saveCursorToFile(originalCursor)
	if err != nil {
		t.Fatalf("Failed to save cursor: %v", err)
	}

	if _, err := os.Stat(cursorFilePath); os.IsNotExist(err) {
		t.Fatalf("Cursor file was not created")
	}

	loadedCursor := LoadCursorFromFile(logger, cursorFilePath)
	if loadedCursor == nil {
		t.Fatalf("Failed to load cursor from file")
	}

	originalStr := originalCursor.String()
	loadedStr := loadedCursor.String()

	if originalStr != loadedStr {
		t.Errorf("Cursor mismatch:\nOriginal: %s\nLoaded:   %s", originalStr, loadedStr)
	}
}

func TestCursorLoadNonExistentFile(t *testing.T) {
	logger := zaptest.NewLogger(t)

	cursor := LoadCursorFromFile(logger, "/tmp/non-existent-cursor-file.cursor")

	if cursor != nil {
		t.Errorf("Expected nil cursor for non-existent file, got: %v", cursor)
	}
}

func TestCursorLoadEmptyFile(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	emptyFilePath := filepath.Join(tempDir, "empty.cursor")

	err := os.WriteFile(emptyFilePath, []byte(""), 0644)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	cursor := LoadCursorFromFile(logger, emptyFilePath)

	if cursor != nil {
		t.Errorf("Expected nil cursor for empty file, got: %v", cursor)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	logger := zaptest.NewLogger(t)

	testCursors := []string{
		"XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE=",
		"R6y3vDTh2Z9jZMGHJVbBeqWwLpc_DFhvVQvlKhhIjIGFpQayof_gfSsIPB6Hl-GTuESoThquiqirYBl_u6EPuYS-kedg7lc7LyllzYPs-7TvePP7MAMRAu4WKJbsJKWRJmjUNQ2zGdRRo9CnOKeuATZVZo5Hc2Pm220PpoYic8pD8C0=",
	}

	for i, testCursorStr := range testCursors {
		t.Run(t.Name()+"_"+string(rune(i)), func(t *testing.T) {

			tempDir := t.TempDir()
			cursorFilePath := filepath.Join(tempDir, "roundtrip.cursor")

			handler := &Handler{
				cursorFilePath: cursorFilePath,
				logger:         logger,
			}

			originalCursor, err := sink.NewCursor(testCursorStr)
			if err != nil {
				t.Fatalf("Failed to create cursor: %v", err)
			}

			for round := 0; round < 3; round++ {

				err = handler.saveCursorToFile(originalCursor)
				if err != nil {
					t.Fatalf("Round %d: Failed to save cursor: %v", round, err)
				}

				loadedCursor := LoadCursorFromFile(logger, cursorFilePath)
				if loadedCursor == nil {
					t.Fatalf("Round %d: Failed to load cursor", round)
				}

				if originalCursor.String() != loadedCursor.String() {
					t.Errorf("Round %d: Cursor mismatch", round)
				}

				originalCursor = loadedCursor
			}
		})
	}
}
