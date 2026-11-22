package receiver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

type errResolver struct{ err error }

func (r errResolver) LookupNodeID(context.Context, string) (string, bool, error) {
	return "", false, r.err
}

// A saturated (ErrBusy) or closed (ErrClosed) store during the token lookup is
// transient: Unavailable lets the agent reconnect and resend (#533). Other
// store errors stay Internal.
func TestCheckAuth_StoreErrorCodes(t *testing.T) {
	tok := strings.Repeat("a", store.TokenLength)
	for name, tc := range map[string]struct {
		err  error
		want codes.Code
	}{
		"busy":         {store.ErrBusy, codes.Unavailable},
		"closed":       {store.ErrClosed, codes.Unavailable},
		"wrapped busy": {fmt.Errorf("lookup: %w", store.ErrBusy), codes.Unavailable},
		"other":        {errors.New("disk on fire"), codes.Internal},
	} {
		t.Run(name, func(t *testing.T) {
			lim, _ := newTestLimiter()
			_, err := checkAuthLimited(authCtx("203.0.113.7", tok), errResolver{tc.err}, true, lim)
			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v, want %v (err=%v)", got, tc.want, err)
			}
		})
	}
}
