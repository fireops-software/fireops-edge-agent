package error

import "fmt"

type ErrDockerApi string

// Error implements error.
func (e ErrDockerApi) Error() string {
	return fmt.Sprintf("ErrDockerApi: %s", string(e))
}

func NewErrDockerApi(format string, args ...any) error {
	return ErrDockerApi(fmt.Sprintf(format, args...))
}
