package receiver

import "github.com/flipslidersand/sentinel-mesh/internal/authlimit"

// The failed-auth throttle lives in internal/authlimit (shared with httpauth, #386).
type authFailLimiter = authlimit.Limiter

const authFailBurst = authlimit.Burst

func newAuthFailLimiter() *authFailLimiter { return authlimit.New() }
