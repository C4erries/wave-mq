//go:build race

package controller

func raceDetectorEnabled() bool {
	return true
}
