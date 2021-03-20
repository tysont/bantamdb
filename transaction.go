package main

type Transaction struct {
	Checks []string
	Writes []*Document
}

func NewTransaction(checks []string, writes []*Document) *Transaction {
	return &Transaction{
		Checks: checks,
		Writes: writes,
	}
}