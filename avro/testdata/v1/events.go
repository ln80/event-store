package bank

import "github.com/ln80/event-store/event"

type User struct {
	ID    string `pii:"subjectID"`
	Email string `pii:"data"`
}

type AccountOpened struct {
	ID    string
	Owner User `pii:"dive"`
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

func Register() {
	event.NewRegister("bank").Set(AccountOpened{})
	event.NewRegister("bank").Set(MoneyDeposited{})
	event.NewRegister("bank").Set(MoneyWithdrawn{})
}

func Clear() {
	event.NewRegister("bank").Clear()
}
