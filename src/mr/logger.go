package mr

import "fmt"

type Logger struct {
	Debug bool
}

func (l *Logger) Println(args ...interface{}) {
	if l.Debug {
		fmt.Println(args...)
	}
}

func (l *Logger) Printf(format string, args ...interface{}) {
	if l.Debug {
		fmt.Printf(format, args...)
	}
}

func (l *Logger) Print(args ...interface{}) {
	if l.Debug {
		fmt.Print(args...)
	}
}

var logger = &Logger{Debug: false}
