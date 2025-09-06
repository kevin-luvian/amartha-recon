package model

import "fmt"

type Transaction struct {
	Source     string
	Id         string
	Type       string
	Amount     float64 // 10.51 max 2 decimal places
	Date       string  // YYYY-MM-DD
	DateEpoch  int64   // Unix epoch time
	ParseError error
}

func (t Transaction) Hash() (string, []string) {
	return t.GetHashById(), []string{t.Date, t.Type, fmt.Sprintf("%.2f", t.Amount), t.Id}
}

func (t *Transaction) GetHashById() string {
	return fmt.Sprintf("%s|%s|%s", t.Date, t.Type, t.Id)
}

func (t *Transaction) GetKeySearchByDate() []string {
	return []string{t.Date, t.Type}
}

func (t *Transaction) GetKeySearchByAmount() []string {
	return []string{t.Date, t.Type, fmt.Sprintf("%.2f", t.Amount)}
}
