package testutil

type Event struct{ Val string }

type Event2 struct{ Val string }

func (e *Event2) EvDests() []string {
	return []string{"dest_2"}
}
