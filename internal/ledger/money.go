package ledger

import "errors"

func Notional(quantity, price, fee int64) (int64, error) {
	if quantity < 1 || quantity > 1000000 || price < 1 || price > 1000000000 || fee < 0 || fee > 1000000000 {
		return 0, errors.New("quantity, price or fee outside supported bounds")
	}
	return quantity * price, nil
}
