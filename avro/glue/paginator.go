package glue

import (
	"context"
	"fmt"
)

type paginator[I any, O any, T any] struct {
	input     *I
	nextToken *T
	firstPage bool
	getToken  func(*O) *T
	setToken  func(*I, *T) *I
	query     func(context.Context, *I) (*O, error)
}

func newPaginator[I any, O any, T any](i *I, getToken func(*O) *T, setToken func(*I, *T) *I, query func(context.Context, *I) (*O, error)) *paginator[I, O, T] {
	return &paginator[I, O, T]{
		input:     i,
		firstPage: true,
		getToken:  getToken,
		setToken:  setToken,
		query:     query,
	}
}

func (p *paginator[I, O, T]) HasMorePages() bool {
	return p.firstPage || p.nextToken != nil
}

func (p *paginator[I, O, T]) NextPage(ctx context.Context) (*O, error) {
	var o *O
	if !p.HasMorePages() {
		return o, fmt.Errorf("no more pages available")
	}

	o, err := p.query(ctx, p.input)
	if err != nil {
		return o, err
	}

	p.firstPage = false
	p.nextToken = p.getToken(o)
	p.input = p.setToken(p.input, p.nextToken)

	return o, nil
}
