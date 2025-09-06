package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"project/internal/model"
	"project/pkg/ingester"
	"reflect"
	"testing"
)

type TestReconService_MockParser struct{}

func (t *TestReconService_MockParser) Parse(record map[string]string) model.Transaction {
	return model.Transaction{Id: record["id"], Type: record["type"]}
}

type TestReconService_ReconcileArgs struct {
	Label          string
	InternalSource string
	Args           func() <-chan model.Transaction
	CheckExpected  func(rt []ReconTransaction) error
}

func TestReconService_NewReconService(t *testing.T) {
	newService := NewReconService(NewReconServiceOpts{})

	if len(newService.FilterDateRange) != 0 {
		t.Fatalf("Expected 0, got %d", len(newService.FilterDateRange))
	}

	newService = NewReconService(NewReconServiceOpts{
		FilterDateRange: []string{"2025-01-01", "2025-01-10"},
	})

	expectedEpochs := []int64{1735689600000, 1736467200000}
	if !reflect.DeepEqual(newService.filterDateRangeEpoch, expectedEpochs) {
		t.Fatalf("Expected %v, got %v", expectedEpochs, newService.filterDateRangeEpoch)
	}
}

func TestReconService_Reconcile(t *testing.T) {
	ctx := context.Background()
	testCases := []TestReconService_ReconcileArgs{{
		Label: "Success matched",
		Args: func() <-chan model.Transaction {
			txns := []model.Transaction{
				{
					Id:        "txn_1",
					Amount:    10,
					Date:      "2025-01-01",
					DateEpoch: 1735689600000,
				},
				{
					Id:        "txn_2",
					Amount:    10,
					Date:      "2025-01-02",
					DateEpoch: 1735689600000,
				},
				{
					Id:        "txn_3",
					Amount:    10,
					Date:      "2025-01-03",
					DateEpoch: 1735689600000,
				},
				{
					Id:        "txn_4",
					Amount:    10,
					Date:      "2025-01-04",
					DateEpoch: 1735689600000,
				},
			}
			outChan := make(chan model.Transaction, len(txns)*2)
			for _, txn := range txns {
				txn.Source = "internal"
				outChan <- txn
				txn.Source = "external"
				outChan <- txn
			}
			close(outChan)
			return outChan
		},
		CheckExpected: func(rt []ReconTransaction) error {
			if len(rt) != 4 {
				return fmt.Errorf("Expected 4, got %d", len(rt))
			}

			for _, txn := range rt {
				if !txn.IsMatched {
					return fmt.Errorf("Expected matched, got %v", txn)
				}
			}

			return nil
		},
	}, {
		Label: "Success matched by amount",
		Args: func() <-chan model.Transaction {
			outChan := make(chan model.Transaction, 2)
			outChan <- model.Transaction{
				Source:    "internal",
				Id:        "txn_1",
				Amount:    10,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			outChan <- model.Transaction{
				Source:    "external",
				Id:        "ext_txn_1",
				Amount:    10,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			close(outChan)
			return outChan
		},
		CheckExpected: func(rt []ReconTransaction) error {
			if len(rt) != 1 {
				return fmt.Errorf("Expected 1, got %d", len(rt))
			}

			txn := rt[0]
			if !txn.IsMatched {
				return fmt.Errorf("Expected matched, got %v", txn)
			}

			return nil
		},
	}, {
		Label: "Success matched by date",
		Args: func() <-chan model.Transaction {
			outChan := make(chan model.Transaction, 2)
			outChan <- model.Transaction{
				Source:    "internal",
				Id:        "txn_1",
				Amount:    10,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			outChan <- model.Transaction{
				Source:    "external",
				Id:        "ext_txn_1",
				Amount:    1,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			close(outChan)
			return outChan
		},
		CheckExpected: func(rt []ReconTransaction) error {
			if len(rt) != 1 {
				return fmt.Errorf("Expected 1, got %d", len(rt))
			}

			txn := rt[0]
			if !txn.IsMatched {
				return fmt.Errorf("Expected matched, got %v", txn)
			}

			return nil
		},
	}, {
		Label: "mismatched by id",
		Args: func() <-chan model.Transaction {
			outChan := make(chan model.Transaction, 3)
			outChan <- model.Transaction{
				Source:    "internal",
				Id:        "txn_1",
				Amount:    10,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			outChan <- model.Transaction{
				Source:    "external",
				Id:        "ext_txn_1",
				Amount:    15,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			outChan <- model.Transaction{
				Source:    "external",
				Id:        "ext_txn_2",
				Amount:    15,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			close(outChan)
			return outChan
		},
		CheckExpected: func(rt []ReconTransaction) error {
			if len(rt) != 3 {
				return fmt.Errorf("Expected 3, got %d", len(rt))
			}

			for _, txn := range rt {
				if txn.IsMatched {
					return fmt.Errorf("Expected unmatched, got %v", txn)
				}
			}

			return nil
		},
	}, {
		Label: "mismatched by parse error",
		Args: func() <-chan model.Transaction {
			outChan := make(chan model.Transaction, 1)
			outChan <- model.Transaction{
				Source:     "internal",
				Id:         "txn_1",
				Amount:     10,
				Date:       "2025-01-0",
				DateEpoch:  1735689600000,
				ParseError: fmt.Errorf("test parse error"),
			}
			close(outChan)
			return outChan
		},
		CheckExpected: func(rt []ReconTransaction) error {
			if len(rt) != 1 {
				return fmt.Errorf("Expected 1, got %d", len(rt))
			}

			for _, txn := range rt {
				if txn.IsMatched {
					return fmt.Errorf("Expected mismatched, got %v", txn)
				}
			}

			return nil
		},
	}}

	for _, testCase := range testCases {
		newService := NewReconService(NewReconServiceOpts{
			Ctx: ctx,
		})
		newService.internalSource = "internal"

		inChan := testCase.Args()
		outChan := newService.Reconcile(inChan)

		outputTransactions := []ReconTransaction{}
		for t := range outChan {
			outputTransactions = append(outputTransactions, t)
		}

		err := testCase.CheckExpected(outputTransactions)
		if err != nil {
			t.Errorf("[%s] %v", testCase.Label, err)
		}
	}
}

func TestReconService_ReadInternalCsv(t *testing.T) {
	// Create a temporary CSV file
	ctx := context.Background()
	dir := os.TempDir()
	filePath := filepath.Join(dir, "test.csv")
	content := "id,type\n30,one\n25,two\n"

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create temp csv file: %v", err)
	}
	defer os.Remove(filePath)

	newService := NewReconService(NewReconServiceOpts{
		Ctx:         ctx,
		CsvIngester: ingester.NewCsvIngester(),
	})

	txnChan := newService.ReadInternalCsv(ReconCsvDetail{
		Source:      "test",
		CsvFilepath: filePath,
		Parser:      &TestReconService_MockParser{},
	})

	i := 0
	for txn := range txnChan {
		if i == 0 {
			if txn.Id != "30" {
				t.Fatalf("Expected 30, got %s", txn.Id)
			}
			if txn.Type != "one" {
				t.Fatalf("Expected one, got %s", txn.Type)
			}
		}

		i += 1
	}
}

func TestReconService_ReadExternalCsv(t *testing.T) {
	// Create a temporary CSV file
	ctx := context.Background()
	dir := os.TempDir()
	filePath := filepath.Join(dir, "test.csv")
	content := "id,type\n30,one\n25,two\n"

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create temp csv file: %v", err)
	}
	defer os.Remove(filePath)

	newService := NewReconService(NewReconServiceOpts{
		Ctx:         ctx,
		CsvIngester: ingester.NewCsvIngester(),
	})

	txnChan := newService.ReadExternalCsv(ReconCsvDetail{
		Source:      "test",
		CsvFilepath: filePath,
		Parser:      &TestReconService_MockParser{},
	})

	i := 0
	for txn := range txnChan {
		if i == 0 {
			if txn.Id != "30" {
				t.Fatalf("Expected 30, got %s", txn.Id)
			}
			if txn.Type != "one" {
				t.Fatalf("Expected one, got %s", txn.Type)
			}
		}

		i += 1
	}
}
