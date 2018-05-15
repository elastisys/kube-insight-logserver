package log

import (
	"flag"
	"fmt"
	"log"
	"os"
)

const (
	// ErrorLevel represents ERROR log level
	ErrorLevel int = iota
	// WarnLevel represents WARN log level
	WarnLevel
	// InfoLevel represents INFO log level
	InfoLevel
	// DebugLevel represents DEBUG log level
	DebugLevel
	// TraceLevel represents TRACE log level
	TraceLevel
)

var (
	// globalLevel is the global logging level.
	globalLevel = InfoLevel

	// traceLog can be used as a global trace-level logger
	traceLog *log.Logger
	// debugLog can be used as a global debug-level logger
	debugLog *log.Logger
	// infoLog can be used as a global info-level logger
	infoLog *log.Logger
	// warnLog can be used as a global warn-level logger
	warnLog *log.Logger
	// errorLog can be used as a global error-level logger
	errorLog *log.Logger
	// fatalLog can be used as a global fatal-level logger
	fatalLog *log.Logger
)

// SetLevel sets the global logging level. Must be one of
// `TraceLevel`, `DebugLevel`, `InfoLevel`, `WarnLevel` and `ErrorLevel`.
func SetLevel(level int) {
	switch globalLevel {
	case DebugLevel, InfoLevel, WarnLevel, ErrorLevel:
		globalLevel = level
	default:
		log.Fatalf("unrecognized log level: %d", globalLevel)
	}
}

// Level returns the currently set global logging level.
func Level() int {
	return globalLevel
}

func init() {
	// Add a command-line flag
	flag.IntVar(&globalLevel, "log-level", InfoLevel,
		"Set the log-level to use. One of ERROR: 0, WARN: 1, INFO: 2, DEBUG: 3, TRACE: 4. Default: 2")

	traceLog = log.New(os.Stdout,
		"[T] ", log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lshortfile)
	debugLog = log.New(os.Stdout,
		"[D] ", log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lshortfile)
	infoLog = log.New(os.Stdout,
		"[I] ", log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lshortfile)
	warnLog = log.New(os.Stdout,
		"[W] ", log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lshortfile)
	errorLog = log.New(os.Stdout,
		"[E] ", log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lshortfile)
	fatalLog = log.New(os.Stdout,
		"[F] ", log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lshortfile)
}

// Tracef prints a trace-level message.
func Tracef(format string, v ...interface{}) {
	if globalLevel >= TraceLevel {
		traceLog.Output(2, fmt.Sprintf(format, v...))
	}
}

// Debugf prints a debug-level message.
func Debugf(format string, v ...interface{}) {
	if globalLevel >= DebugLevel {
		debugLog.Output(2, fmt.Sprintf(format, v...))
	}
}

// Infof prints an info-level message.
func Infof(format string, v ...interface{}) {
	if globalLevel >= InfoLevel {
		infoLog.Output(2, fmt.Sprintf(format, v...))
	}
}

// Warnf prints a warn-level message.
func Warnf(format string, v ...interface{}) {
	if globalLevel >= WarnLevel {
		warnLog.Output(2, fmt.Sprintf(format, v...))
	}
}

// Errorf prints an error-level message.
func Errorf(format string, v ...interface{}) {
	if globalLevel >= ErrorLevel {
		errorLog.Output(2, fmt.Sprintf(format, v...))
	}
}

// Fatalf prints a fatal message and then exits with non-zero exit status.
func Fatalf(format string, v ...interface{}) {
	fatalLog.Output(2, fmt.Sprintf(format, v...))
	os.Exit(1)
}
