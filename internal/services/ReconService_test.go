package services

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kevin-luvian/amartha-recon/internal/model"
	"github.com/kevin-luvian/amartha-recon/pkg/ingester"
)

type TestReconService_MockParser struct{}

func (t *TestReconService_MockParser) Parse(record map[string]string) model.Transaction {
	return model.Transaction{Id: record["id"], Type: record["type"]}
}

func TestReconService_NewReconService(t *testing.T) {
	newService, _ := NewReconService(NewReconServiceOpts{})

	if len(newService.FilterDateRange) != 0 {
		t.Fatalf("Expected 0, got %d", len(newService.FilterDateRange))
	}

	newService, _ = NewReconService(NewReconServiceOpts{
		FilterDateRange: []string{"2025-01-01", "2025-01-10"},
	})

	expectedEpochs := []int64{1735689600000, 1736467200000}
	if !reflect.DeepEqual(newService.filterDateRangeEpoch, expectedEpochs) {
		t.Fatalf("Expected %v, got %v", expectedEpochs, newService.filterDateRangeEpoch)
	}
}

type TestReconService_ReconcileArgs struct {
	Label          string
	InternalSource string
	Args           func() <-chan model.Transaction
	CheckExpected  func(rt []ReconTransaction) error
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
			if len(rt) != 2 {
				return fmt.Errorf("Expected 2, got %d", len(rt))
			}

			for _, txn := range rt {
				if !txn.IsMatched {
					return fmt.Errorf("Expected matched, got %v", txn)
				}
			}

			return nil
		},
	}, {
		Label: "Success matched by id discrepancy amount",
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
				Id:        "txn_1",
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

			discrepancy := math.Abs(txn.Amount - txn.OtherTransaction.Amount)
			if discrepancy != 9 {
				return fmt.Errorf("Expected 9, got %.2f", discrepancy)
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
		Label: "mismatched by date",
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
				Amount:    1,
				Date:      "2025-01-01",
				DateEpoch: 1735689600000,
			}
			outChan <- model.Transaction{
				Source:    "external",
				Id:        "ext_txn_2",
				Amount:    5,
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
					return fmt.Errorf("Expected mismatched, got %v", txn)
				}
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
		newService, _ := NewReconService(NewReconServiceOpts{
			Ctx: ctx,
		})
		newService.internalSource = "internal"

		inChan := testCase.Args()
		outChan, _ := newService.Reconcile(inChan)

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

type TestReconService_PassThroughSummaryArgs struct {
	Label         string
	Args          []ReconTransaction
	CheckExpected func(rs *ReconSummary) error
}

func TestReconService_PassThroughSummary(t *testing.T) {
	ctx := context.Background()
	testCases := []TestReconService_PassThroughSummaryArgs{{
		Label: "Matched counted",
		Args: []ReconTransaction{
			{
				Transaction: model.Transaction{Id: "1"},
				IsMatched:   true,
			},
		},
		CheckExpected: func(rs *ReconSummary) error {
			if rs.TotalMatched != 2 {
				return fmt.Errorf("Expected 2, got %d", rs.TotalMatched)
			}

			if rs.TotalDiscrepancy != 0 {
				return fmt.Errorf("Expected 0, got %.2f", rs.TotalDiscrepancy)
			}

			return nil
		},
	}, {
		Label: "Matched discrepancy",
		Args: []ReconTransaction{
			{
				Transaction:      model.Transaction{Id: "1"},
				OtherTransaction: model.Transaction{Id: "2", Amount: 10},
				IsMatched:        true,
			},
		},
		CheckExpected: func(rs *ReconSummary) error {
			if rs.TotalDiscrepancy != 10 {
				return fmt.Errorf("Expected 10, got %.2f", rs.TotalDiscrepancy)
			}

			return nil
		},
	}, {
		Label: "Mismatch Sources",
		Args: []ReconTransaction{
			{
				Transaction: model.Transaction{Id: "1", Source: "amartha"},
				IsMatched:   false,
			},
			{
				Transaction: model.Transaction{Id: "2", Source: "bca"},
				IsMatched:   false,
			},
		},
		CheckExpected: func(rs *ReconSummary) error {
			if rs.TotalMismatched != 2 {
				return fmt.Errorf("Expected total mismatch 2, got %d", rs.TotalMismatched)
			}

			sourceTotal := rs.TotalMismatchBySource["amartha"]
			if sourceTotal != 1 {
				return fmt.Errorf("Expected internal source 1, got %d", sourceTotal)
			}

			sourceTotal = rs.TotalMismatchBySource["bca"]
			if sourceTotal != 1 {
				return fmt.Errorf("Expected external source 1, got %d", sourceTotal)
			}

			return nil
		},
	}}

	for _, testCase := range testCases {
		newService, _ := NewReconService(NewReconServiceOpts{Ctx: ctx})
		summary := NewReconSummary()

		transactions := testCase.Args
		inChan := make(chan ReconTransaction, len(transactions))
		for _, txn := range transactions {
			inChan <- txn
		}
		close(inChan)

		outChan := newService.PassThroughSummary(inChan, summary)

		i := 0
		for txn := range outChan {
			if txn.Id != transactions[i].Id {
				t.Fatalf("Expected %v, got %v", transactions[i], txn)
			}
			i += 1
		}

		err := testCase.CheckExpected(summary)
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
	content := "id,type\n30,one\n"

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create temp csv file: %v", err)
	}
	defer os.Remove(filePath)

	newService, _ := NewReconService(NewReconServiceOpts{
		Ctx:         ctx,
		CsvIngester: ingester.NewCsvIngester(),
	})

	txnChan, _ := newService.ReadInternalCsv(ReconCsvDetail{
		Source:      "test",
		CsvFilepath: filePath,
		Parser:      &TestReconService_MockParser{},
	})

	for txn := range txnChan {
		if txn.Id != "30" {
			t.Fatalf("Expected 30, got %s", txn.Id)
		}
		if txn.Type != "one" {
			t.Fatalf("Expected one, got %s", txn.Type)
		}
	}
}

func TestReconService_ReadExternalCsv(t *testing.T) {
	// Create a temporary CSV file
	ctx := context.Background()
	dir := os.TempDir()
	filePath := filepath.Join(dir, "test.csv")
	content := "id,type\n30,one\n"

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create temp csv file: %v", err)
	}
	defer os.Remove(filePath)

	newService, _ := NewReconService(NewReconServiceOpts{
		Ctx:         ctx,
		CsvIngester: ingester.NewCsvIngester(),
	})

	txnChan, _ := newService.ReadExternalCsv(ReconCsvDetail{
		Source:      "test",
		CsvFilepath: filePath,
		Parser:      &TestReconService_MockParser{},
	})

	for txn := range txnChan {
		if txn.Id != "30" {
			t.Fatalf("Expected 30, got %s", txn.Id)
		}
		if txn.Type != "one" {
			t.Fatalf("Expected one, got %s", txn.Type)
		}
	}
}

func TestReconService_WriteToCsv(t *testing.T) {
	// Create a temporary CSV file
	ctx := context.Background()
	dir := os.TempDir()
	filePath := filepath.Join(dir, "test.csv")
	defer os.Remove(filePath)

	newService, _ := NewReconService(NewReconServiceOpts{
		Ctx:         ctx,
		CsvIngester: ingester.NewCsvIngester(),
	})

	txnChan := make(chan ReconTransaction, 1)
	txnChan <- ReconTransaction{
		Transaction: model.Transaction{
			Source: "test",
			Id:     "txn_1",
			Date:   "2025-01-01",
		},
		Remark: "remarks",
	}
	close(txnChan)

	err := newService.WriteToCsv(filePath, txnChan)
	if err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal(err)
	}

	csvStr := string(b)
	expected := "source,id,type,amount,date,remark\ntest,txn_1,,0.00,2025-01-01,remarks\n"
	if csvStr != expected {
		t.Fatalf("Expected %s, got %s", expected, csvStr)
	}
}
