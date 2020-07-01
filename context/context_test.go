package context_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/google/go-cmp/cmp"
	cecontext "github.com/silverswords/whisper/context"
)

func TestTargetContext(t *testing.T) {
	exampleDotCom, _ := url.Parse("http://example.com")

	testCases := map[string]struct {
		target string
		ctx    context.Context
		want   *url.URL
	}{
		"todo context, set url": {
			ctx:    context.TODO(),
			target: "http://example.com",
			want:   exampleDotCom,
		},
		"bad url": {
			ctx:    context.TODO(),
			target: "%",
		},
		"already set target": {
			ctx:    cecontext.WithTarget(context.TODO(), "http://example2.com"),
			target: "http://example.com",
			want:   exampleDotCom,
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {

			ctx := cecontext.WithTarget(tc.ctx, tc.target)

			got := cecontext.TargetFrom(ctx)

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("unexpected (-want, +got) = %v", diff)
			}
		})
	}
}
