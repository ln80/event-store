package bank

type Mutator func(evt any)

type Aggregate struct {
	evChanges []any
	mutator   Mutator
}

func (a *Aggregate) Apply(evt any) {
	if a.evChanges == nil {
		a.evChanges = []any{}
	}
	a.evChanges = append(a.evChanges, evt)
	a.mutate(evt)
}

func (a *Aggregate) SetMutator(mutator Mutator) {
	a.mutator = mutator
}

func (a *Aggregate) mutate(evt any) {
	if a.mutator == nil {
		panic("invalid aggregate, state mutator not defined")
	}

	a.mutator(evt)
}

func (a *Aggregate) EvClearChanges() []any {
	defer func() {
		a.evChanges = nil
	}()

	return a.evChanges
}
