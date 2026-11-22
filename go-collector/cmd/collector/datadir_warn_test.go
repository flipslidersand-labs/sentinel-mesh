package main

import "testing"

func TestIsTmpDataDir(t *testing.T) {
	cases := map[string]bool{
		"/tmp/sentinel-data": true,
		"/tmp/../tmp/x":      true,
		"/tmp":               true,
		"/var/lib/sentinel":  false,
		"/tmpfoo/x":          false,
		"/home/u/tmp/data":   false,
		"/var/tmp/data":      false,
	}
	for in, want := range cases {
		if got := isTmpDataDir(in); got != want {
			t.Errorf("isTmpDataDir(%q) = %v, want %v", in, got, want)
		}
	}
}
