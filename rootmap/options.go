package rootmap

/*
Options for the SQLRootmap.
*/

type config struct {
	reservationSize int
}

// Option is a function that modifies the SQLRootmap configuration.
type Option func(*config)

// WithReservationSize sets the reservation size for the SQLRootmap.
func WithReservationSize(size int) Option {
	return func(c *config) {
		c.reservationSize = size
	}
}
