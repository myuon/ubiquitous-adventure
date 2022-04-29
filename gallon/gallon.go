package gallon

import "context"

type Gallon struct {
	input  InputPlugin
	output OutputPlugin
}

func (g Gallon) Run() error {
	pipe := NewPipe()

	if err := g.output.Connect(
		context.TODO(),
		pipe,
	); err != nil {
		return err
	}

	if err := g.input.Connect(
		context.TODO(),
		pipe,
	); err != nil {
		return err
	}

	return nil
}

func NewGallon(
	input InputPlugin,
	output OutputPlugin,
) Gallon {
	return Gallon{
		input:  input,
		output: output,
	}
}
