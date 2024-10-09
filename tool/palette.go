package tool

import (
	avro_tool "github.com/ln80/event-store/tool/avro"
	internal "github.com/ln80/event-store/tool/internal"
)

type Palette struct {
	printer internal.TaskPrinter
}

func NewPalette() *Palette {
	return &Palette{
		printer: &NoPrinter{},
	}
}

func (p *Palette) SetPrinter(printer internal.TaskPrinter) {
	p.printer = printer
}

func (p *Palette) AVRO() *avro_tool.JobExecuter {
	return avro_tool.NewJobExecuter(p.printer)
}
