package gallon

import "context"

type InputPlugin interface {
	Connect(
		ctx context.Context,
		writer WriteCloser,
	) error
}

type OutputPlugin interface {
	Connect(
		ctx context.Context,
		reader Reader,
	) error
}
