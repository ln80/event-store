package event

import "fmt"

func Err(err error, stmID string, extra ...any) error {
	return fmt.Errorf("%w: stream=%s extra=%v", err, stmID, extra)
}
