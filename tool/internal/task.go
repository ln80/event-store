package internal

import (
	"context"
	"fmt"
	"io"
	"strings"
)

type Task interface {
	Name() string
	DependsOn() []string
	Run(ctx context.Context, deps ...any) error
}

type task struct {
	Task
	name      string
	dependsOn []string
}

func (t *task) Name() string {
	return t.name
}

func (t *task) DependsOn() []string {
	return t.dependsOn
}

func (t *task) String() string {
	str := t.Name()

	if deps := t.dependsOn; len(deps) > 0 {
		str += " (" + strings.Join(deps, ", ") + ")"
	}

	return str
}

func NewTask(name string) Task {
	return &task{name: name, dependsOn: make([]string, 0)}
}

func TaskFrom[T any](ts []Task) T {
	var tt T
	for _, t := range ts {
		if _t, ok := t.(T); ok {
			tt = _t
			break
		}
	}
	return tt
}

type TaskPrinter interface {
	Message(msg string, t *Task)
	Error(err error, t *Task)
	Task(i int, t Task)
}

type NoPrinter struct{}

// Task implements TaskPrinter.
func (*NoPrinter) Task(i int, t Task) {}

// Error implements TaskPrinter.
func (*NoPrinter) Error(error, *Task) {}

// Print implements TaskPrinter.
func (*NoPrinter) Message(string, *Task) {}

var _ TaskPrinter = &NoPrinter{}

type DefaultPrinter struct {
	Output io.Writer
	Err    io.Writer
}

// Task implements TaskPrinter.
func (p *DefaultPrinter) Task(i int, t Task) {
	fmt.Fprintf(p.Output, "%d. %s\n", i, t.Name())
}

// Error implements TaskPrinter.
func (p *DefaultPrinter) Error(err error, t *Task) {
	if t == nil {
		fmt.Fprintf(p.Err, "error: %v\n", err)
		return
	}
	fmt.Fprintf(p.Err, "%s: error: %v\n", (*t).Name(), err)
}

// Print implements TaskPrinter.
func (p *DefaultPrinter) Message(msg string, t *Task) {
	if t == nil {
		fmt.Fprintf(p.Output, "%s\n", msg)
		return
	}
	fmt.Fprintf(p.Output, "%s: %s\n", msg, (*t).Name())
}

var _ TaskPrinter = &DefaultPrinter{}
