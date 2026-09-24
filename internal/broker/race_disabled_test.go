//go:build !race

package broker_test

func raceDetectorEnabled() bool {
	return false
}
