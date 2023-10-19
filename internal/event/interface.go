package event

type Transformer interface {
	Transform(fn func(curr any) any)
}
