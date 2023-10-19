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
	switch ev := evt.(type) {
	case *AccountOpened:
		a.ID = ev.ID
		a.OpenedAt = ev.At
	case *MoneyDeposited:
		a.Balance += ev.Amount
	case *MoneyWithdrawn:
		a.Balance -= ev.Amount
	}
}
