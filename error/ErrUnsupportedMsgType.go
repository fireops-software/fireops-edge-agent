package error

import "fmt"

type ErrUnsupportedMsgType string

// Error implements error.
func (e ErrUnsupportedMsgType) Error() string {
	return fmt.Sprintf("ErrUnsupportedMsgType: %s", string(e))
}

func NewErrUnsupportedMsgType(format string, args ...any) error {
	return ErrUnsupportedMsgType(fmt.Sprintf(format, args...))
}
