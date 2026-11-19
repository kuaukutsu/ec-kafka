package queue

import "testing"

func TestMessage_GetKey(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given   string
		wantNil bool
		want    string
	}{
		"empty key is nil": {given: "", wantNil: true},
		"key is kept":      {given: "k", want: "k"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := Message{Key: tt.given}.GetKey()

			if tt.wantNil && got != nil {
				t.Fatalf("GetKey() = %#v, want nil", got)
			}

			if !tt.wantNil && string(got) != tt.want {
				t.Fatalf("GetKey() = %q, want %q", got, tt.want)
			}
		})
	}
}
