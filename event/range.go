package event

import "time"

func VersionRange(vers []Version) (from, to Version) {
	if l := len(vers); l > 1 {
		from, to = vers[0], vers[1]
	} else if l == 1 {
		from, to = vers[0], VersionMax
	} else {
		from, to = VersionMin, VersionMax
	}
	return
}

func TimeRange(times []time.Time) (since, until time.Time) {
	if l := len(times); l > 1 {
		since, until = times[0], times[1]
	} else if l == 1 {
		since, until = times[0], time.Now()
	} else {
		since, until = time.Unix(0, 0), time.Now()
	}
	return
}
