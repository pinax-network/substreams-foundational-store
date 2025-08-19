package sink

import (
	"fmt"
	"os"
	"path/filepath"

	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

// LoadCursorFromFile attempts to load a cursor from the specified file.
// Returns nil if the file doesn't exist or if there's an error reading it.
func LoadCursorFromFile(logger *zap.Logger, cursorFilePath string) *sink.Cursor {
	// Use the provided cursor file path or default to "state.cursor" in the current directory
	var cursorPath string
	if cursorFilePath != "" {
		cursorPath = cursorFilePath
	} else {
		// Use the current working directory for simplicity
		dir, err := os.Getwd()
		if err != nil {
			dir = "."
		}
		cursorPath = filepath.Join(dir, "state.cursor")
	}

	data, err := os.ReadFile(cursorPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn("failed to read cursor file", zap.Error(err))
		}
		return nil
	}

	cursorStr := string(data)
	cursor, err := sink.NewCursor(cursorStr)
	if err != nil {
		logger.Warn("failed to create cursor from string", zap.Error(err))
		return nil
	}

	logger.Info("loaded cursor from file", zap.String("path", cursorPath))
	return cursor
}

// SaveCursorToFile saves the cursor to a file
func SaveCursorToFile(cursor *sink.Cursor, cursorFilePath string, logger *zap.Logger) error {
	if err := sink.WriteCursor(cursorFilePath, cursor); err != nil {
		logger.Error("failed to write cursor to file", zap.String("path", cursorFilePath), zap.Error(err))
		return fmt.Errorf("writing cursor to file: %w", err)
	}
	return nil
}
