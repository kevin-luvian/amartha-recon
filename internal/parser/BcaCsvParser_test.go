package parser

import (
	"fmt"
	"testing"

	"github.com/kevin-luvian/amartha-recon/internal/model"
)

type TestBcaCsvParser_ParseArgs struct {
	Label         string
	Args          map[string]string
	CheckExpected func(txn model.Transaction) error
}

func TestBcaCsvParser_Parse(t *testing.T) {
	testCases := []TestBcaCsvParser_ParseArgs{{
		Label: "match id",
		Args: map[string]string{
			"ext_id": "123",
		},
		CheckExpected: func(txn model.Transaction) error {
			if txn.Id != "123" {
				return fmt.Errorf("Expected 123, got %s", txn.Id)
			}
			return nil
		},
	}, {
		Label: "match amount debit",
		Args: map[string]string{
			"amount": "-7.05",
		},
		CheckExpected: func(txn model.Transaction) error {
			if txn.Amount != 7.05 {
				return fmt.Errorf("Expected 7.05, got %.2f", txn.Amount)
			}
			if txn.Type != "DEBIT" {
				return fmt.Errorf("Expected DEBIT, got %s", txn.Type)
			}
			return nil
		},
	}, {
		Label: "match amount credit",
		Args: map[string]string{
			"amount": "7.05",
		},
		CheckExpected: func(txn model.Transaction) error {
			if txn.Amount != 7.05 {
				return fmt.Errorf("Expected 7.05, got %.2f", txn.Amount)
			}
			if txn.Type != "CREDIT" {
				return fmt.Errorf("Expected CREDIT, got %s", txn.Type)
			}
			return nil
		},
	}, {
		Label: "error parsing date",
		Args: map[string]string{
			"date": "2025.01.01",
		},
		CheckExpected: func(txn model.Transaction) error {
			if txn.ParseError == nil {
				return fmt.Errorf("Expected ParseError, got nil")
			}
			if txn.Date != "0001-01-01" {
				return fmt.Errorf("Expected 0001-01-01, got %s", txn.Date)
			}
			return nil
		},
	}}

	newParser := NewBcaParser()
	for _, testCase := range testCases {
		record := testCase.Args
		txn := newParser.Parse(record)
		err := testCase.CheckExpected(txn)
		if err != nil {
			t.Errorf("[%s] %v", testCase.Label, err)
		}
	}
}
