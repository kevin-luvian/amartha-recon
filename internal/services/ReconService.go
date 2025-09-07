package services

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/kevin-luvian/amartha-recon/internal/model"
	"github.com/kevin-luvian/amartha-recon/internal/parser"
	"github.com/kevin-luvian/amartha-recon/pkg/ingester"
	"github.com/kevin-luvian/amartha-recon/pkg/pipeline"
	"github.com/kevin-luvian/amartha-recon/pkg/storage"
)

type ReconSummary struct {
	TotalMatched          int
	TotalMismatched       int
	TotalDiscrepancy      float64
	TotalMismatchBySource map[string]int
}

func NewReconSummary() *ReconSummary {
	return &ReconSummary{
		TotalMismatchBySource: make(map[string]int),
	}
}

type ReconTransaction struct {
	model.Transaction
	OtherTransaction model.Transaction
	IsMatched        bool
	IsError          bool
	Remark           string
}

type ReconCsvDetail struct {
	Source      string
	CsvFilepath string
	Parser      parser.IParseAble[model.Transaction]
}

type ReconService struct {
	Ctx                  context.Context
	CsvIngester          ingester.ICsvIngester
	FilterDateRange      []string
	filterDateRangeEpoch []int64
	internalSource       string
	externalSources      []string
	internalTable        storage.HashTable
	externalTable        storage.HashTable
}

type NewReconServiceOpts struct {
	Ctx             context.Context
	CsvIngester     ingester.ICsvIngester
	FilterDateRange []string
}

func NewReconService(opts NewReconServiceOpts) (*ReconService, error) {
	service := &ReconService{
		Ctx:             opts.Ctx,
		CsvIngester:     opts.CsvIngester,
		FilterDateRange: opts.FilterDateRange,
		internalTable:   *storage.NewHashTable(),
		externalTable:   *storage.NewHashTable(),
	}

	if len(opts.FilterDateRange) == 2 {
		startDate, err := time.Parse(time.DateOnly, opts.FilterDateRange[0])
		if err != nil {
			return service, err
		}

		endDate, err := time.Parse(time.DateOnly, opts.FilterDateRange[1])
		if err != nil {
			return service, err
		}

		service.filterDateRangeEpoch = []int64{startDate.UnixMilli(), endDate.UnixMilli()}
	}

	return service, nil
}

func (r *ReconService) ReadInternalCsv(detail ReconCsvDetail) (<-chan model.Transaction, error) {
	outputChan := make(chan model.Transaction, 10)

	if r.internalSource != "" {
		return outputChan, fmt.Errorf("only one internal csv source expected")
	}
	r.internalSource = detail.Source

	readChan, err := r.CsvIngester.Read(r.Ctx, detail.CsvFilepath)
	if err != nil {
		return outputChan, err
	}

	pipeline.GetTransformerChans(
		readChan,
		outputChan,
		4,
		detail.Parser.Parse,
	)

	return outputChan, nil
}

func (r *ReconService) ReadExternalCsv(detail ReconCsvDetail) (<-chan model.Transaction, error) {
	r.externalSources = append(r.externalSources, detail.Source)
	outputChan := make(chan model.Transaction, 10)

	readChan, err := r.CsvIngester.Read(r.Ctx, detail.CsvFilepath)
	if err != nil {
		return outputChan, err
	}

	pipeline.GetTransformerChans(
		readChan,
		outputChan,
		4,
		detail.Parser.Parse,
	)

	return outputChan, err
}

func (r *ReconService) Reconcile(transactionChan <-chan model.Transaction) (<-chan ReconTransaction, error) {
	outChan := make(chan ReconTransaction, 10)

	if r.internalSource == "" {
		return outChan, fmt.Errorf("internal source not set")
	}

	go func() {
		defer close(outChan)

		for transaction := range transactionChan {
			// pass error records
			if transaction.ParseError != nil {
				outChan <- ReconTransaction{
					Transaction: transaction,
					IsError:     true,
					Remark:      transaction.ParseError.Error(),
				}
				continue
			}

			if transaction.Source == r.internalSource {
				r.internalTable.Put(transaction)
				reconTransaction, ok := r.processInternalMatching(transaction)
				if ok {
					outChan <- reconTransaction
				}
			} else {
				r.externalTable.Put(transaction)
				reconTransaction, ok := r.processExternalMatching(transaction)
				if ok {
					outChan <- reconTransaction
				}
			}
		}

		for _, externalTransaction := range r.externalTable.Table {
			// Last matching by date, if contains exactly one transaction
			transaction := externalTransaction.(model.Transaction)

			key, isMatch := r.internalTable.IsPathContainsOneValue(transaction.GetKeySearchByDate())
			if isMatch {
				_, isMatch = r.externalTable.IsPathContainsOneValue(transaction.GetKeySearchByDate())
			}

			if isMatch {
				// match exactly one transaction in internal and external by date, flag as match
				intTransaction := r.internalTable.GetById(key)
				r.internalTable.Remove(intTransaction)

				outChan <- ReconTransaction{
					Transaction:      transaction,
					OtherTransaction: intTransaction.(model.Transaction),
					IsMatched:        true,
				}
				continue
			}

			outChan <- ReconTransaction{
				Transaction: externalTransaction.(model.Transaction),
				IsMatched:   false,
				Remark:      "No matching internal transaction found",
			}
		}

		for _, internalTransaction := range r.internalTable.Table {
			outChan <- ReconTransaction{
				Transaction: internalTransaction.(model.Transaction),
				IsMatched:   false,
				Remark:      "No matching external transaction found",
			}
		}
	}()

	return outChan, nil
}

func (r *ReconService) processInternalMatching(transaction model.Transaction) (ReconTransaction, bool) {
	extTransaction := r.externalTable.GetById(transaction.GetHashById())

	if extTransaction == nil {
		extTransaction = r.externalTable.GetFirstMatchByPath(transaction.GetKeySearchByAmount())
	}

	if extTransaction == nil {
		// No match found
		return ReconTransaction{}, false
	}

	// Matched and remove
	r.internalTable.Remove(transaction)
	r.externalTable.Remove(extTransaction)

	return ReconTransaction{
		Transaction:      transaction,
		OtherTransaction: extTransaction.(model.Transaction),
		IsMatched:        true,
	}, true
}

func (r *ReconService) processExternalMatching(transaction model.Transaction) (ReconTransaction, bool) {
	intTransaction := r.internalTable.GetById(transaction.GetHashById())

	if intTransaction == nil {
		intTransaction = r.internalTable.GetFirstMatchByPath(transaction.GetKeySearchByAmount())
	}

	if intTransaction == nil {
		// No match found
		return ReconTransaction{}, false
	}

	// Matched and remove
	r.internalTable.Remove(intTransaction)
	r.externalTable.Remove(transaction)

	return ReconTransaction{
		Transaction:      transaction,
		OtherTransaction: intTransaction.(model.Transaction),
		IsMatched:        true,
	}, true
}

func (r *ReconService) PassThroughSummary(reconTransactionChan <-chan ReconTransaction, summary *ReconSummary) <-chan ReconTransaction {
	return pipeline.TransformChan(reconTransactionChan, func(t ReconTransaction) (ReconTransaction, bool) {
		if t.IsMatched {
			// Count both internal and external matched transactions
			summary.TotalMatched += 2
			summary.TotalDiscrepancy += math.Abs(t.Amount - t.OtherTransaction.Amount)
		} else {
			summary.TotalMismatched += 1
			summary.TotalMismatchBySource[t.Source] += 1
		}

		return t, true
	})
}

func (r *ReconService) FilterMismatched(t ReconTransaction) (ReconTransaction, bool) {
	return t, !t.IsMatched
}

func (r *ReconService) WriteToCsv(filepath string, reconTransactionChan <-chan ReconTransaction) error {
	recordChan := pipeline.TransformChan(reconTransactionChan, func(rt ReconTransaction) (map[string]string, bool) {
		return map[string]string{
			"source": rt.Source,
			"id":     rt.Id,
			"type":   rt.Type,
			"amount": fmt.Sprintf("%.2f", rt.Amount),
			"date":   rt.Date,
			"remark": rt.Remark,
		}, true
	})

	csvHeader := []string{"source", "id", "type", "amount", "date", "remark"}
	return r.CsvIngester.Write(r.Ctx, filepath, csvHeader, recordChan)
}

func (r *ReconService) FilterByDate(record model.Transaction) (model.Transaction, bool) {
	if record.ParseError == nil && (r.filterDateRangeEpoch[0] > record.DateEpoch || r.filterDateRangeEpoch[1] < record.DateEpoch) {
		return record, false
	}

	return record, true
}
