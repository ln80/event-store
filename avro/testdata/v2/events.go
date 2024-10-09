package bank

import "github.com/ln80/event-store/event"

type User struct {
	ID       string `pii:"subjectID"`
	Email    string `pii:"data"`
	BirthDay string `pii:"data,replace=00-00"`
}

type AccountOpened struct {
	ID     string
	Client User `pii:"dive" ev:",aliases=Owner"`
	At     int64
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

const (
	DEFAULT_BIRTH_DAY = "01-01"
)

func Register() {
	event.NewRegister("bank").Set(AccountOpened{
		Client: User{BirthDay: DEFAULT_BIRTH_DAY},
	})
	event.NewRegister("bank").Set(MoneyDeposited{})
	event.NewRegister("bank").Set(MoneyWithdrawn{})
}

func Clear() {
	event.NewRegister("bank").Clear()
}
