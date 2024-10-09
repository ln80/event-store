package tool

import (
	"fmt"
	"io"

	"github.com/ln80/event-store/tool/internal"
)

type NoPrinter struct{}

// Task implements TaskPrinter.
func (*NoPrinter) Task(i int, t internal.Task) {}

// Error implements TaskPrinter.
func (*NoPrinter) Error(error, *internal.Task) {}

// Print implements TaskPrinter.
func (*NoPrinter) Message(string, *internal.Task) {}

var _ internal.TaskPrinter = &NoPrinter{}

type DefaultPrinter struct {
	Output io.Writer
	Err    io.Writer
}

// Task implements TaskPrinter.
func (p *DefaultPrinter) Task(i int, t internal.Task) {
	fmt.Fprintf(p.Output, "%d. %s\n", i, t.Name())
}

// Error implements TaskPrinter.
func (p *DefaultPrinter) Error(err error, t *internal.Task) {
	if t == nil {
		fmt.Fprintf(p.Err, "error: %v\n", err)
		return
	}
	fmt.Fprintf(p.Err, "%s: error: %v\n", (*t).Name(), err)
}

// Print implements TaskPrinter.
func (p *DefaultPrinter) Message(msg string, t *internal.Task) {
	if t == nil {
		fmt.Fprintf(p.Output, "%s\n", msg)
		return
	}
	fmt.Fprintf(p.Output, "%s: %s\n", msg, (*t).Name())
}

var _ internal.TaskPrinter = &DefaultPrinter{}
