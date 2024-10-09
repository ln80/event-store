package bank

type AccountOpened struct {
	ID    string
	Owner string
	At    int64
}

type MoneyDeposited struct {
	Amount    int
	AccountID string
	At        int64
}

type MoneyWithdrawn struct {
	Amount    int
	AccountID string
	At        int64
}

func (a *Account) onEvent(evt any) {
	switch {
	case of(func(ev AccountOpened) {
		a.ID = ev.ID
		a.OpenedAt = ev.At
	}, evt):
	case of(func(ev MoneyDeposited) {
		a.Balance += ev.Amount
	}, evt):
	case of(func(ev MoneyWithdrawn) {
		a.Balance -= ev.Amount
	}, evt):
	}
}

func of[T any](fn func(t T), v any) bool {
	switch v := v.(type) {
	case T:
		fn(v)
	case *T:
		if v == nil {
			return false
		}
		fn(*v)
	default:
		return false
	}
	return true
}
