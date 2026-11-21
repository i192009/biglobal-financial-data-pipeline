package ledger

import "testing"

func TestNotionalExactAndBounds(t *testing.T) {
	cases := []struct {
		quantity, price, fee, want int64
		valid                      bool
	}{{3, 1999, 10, 5997, true}, {1000000, 1000000000, 1000000000, 1000000000000000, true}, {0, 1, 0, 0, false}, {-1, 100, 0, 0, false}, {1, 0, 0, 0, false}, {1000001, 1, 0, 0, false}, {1, 1000000001, 0, 0, false}, {1, 1, -1, 0, false}}
	for _, tc := range cases {
		got, err := Notional(tc.quantity, tc.price, tc.fee)
		if (err == nil) != tc.valid || tc.valid && got != tc.want {
			t.Errorf("Notional(%d,%d,%d)=(%d,%v)", tc.quantity, tc.price, tc.fee, got, err)
		}
	}
}
