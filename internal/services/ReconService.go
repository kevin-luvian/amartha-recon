package services

import (
	"context"
	"fmt"
	"math"
	"project/internal/model"
	"project/internal/parser"
	"project/pkg/ingester"
	"project/pkg/pipeline"
	"project/pkg/storage"
	"time"
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

func (rt ReconTransaction) ToMap() map[string]string {
	return map[string]string{
		"source":     rt.Source,
		"id":         rt.Id,
		"type":       rt.Type,
		"amount":     fmt.Sprintf("%.2f", rt.Amount),
		"date":       rt.Date,
		"is_matched": fmt.Sprintf("%t", rt.IsMatched),
		"remark":     rt.Remark,
	}
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

func NewReconService(opts NewReconServiceOpts) *ReconService {
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
			panic(err)
		}

		endDate, err := time.Parse(time.DateOnly, opts.FilterDateRange[1])
		if err != nil {
			panic(err)
		}

		service.filterDateRangeEpoch = []int64{startDate.UnixMilli(), endDate.UnixMilli()}
	}

	return service
}

func (r *ReconService) ReadInternalCsv(detail ReconCsvDetail) <-chan model.Transaction {
	r.internalSource = detail.Source
	outputChan := make(chan model.Transaction, 10)

	readChan, err := r.CsvIngester.Read(r.Ctx, detail.CsvFilepath)
	if err != nil {
		panic(err)
	}

	pipeline.GetTransformerChans(
		readChan,
		outputChan,
		4,
		detail.Parser.Parse,
	)

	return outputChan
}

func (r *ReconService) ReadExternalCsv(detail ReconCsvDetail) <-chan model.Transaction {
	r.externalSources = append(r.externalSources, detail.Source)
	outputChan := make(chan model.Transaction, 10)

	readChan, err := r.CsvIngester.Read(r.Ctx, detail.CsvFilepath)
	if err != nil {
		panic(err)
	}

	pipeline.GetTransformerChans(
		readChan,
		outputChan,
		4,
		detail.Parser.Parse,
	)

	return outputChan
}

func (r *ReconService) Reconcile(transactionChan <-chan model.Transaction) <-chan ReconTransaction {
	if r.internalSource == "" {
		panic("Internal source not set")
	}

	outChan := make(chan ReconTransaction, 10)

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

			var reconTransaction ReconTransaction
			var ok bool
			if transaction.Source == r.internalSource {
				r.internalTable.Put(transaction)
				reconTransaction, ok = r.processInternalMatching(transaction)
				if !ok {
					continue
				}
			} else {
				r.externalTable.Put(transaction)
				reconTransaction, ok = r.processExternalMatching(transaction)
				if !ok {
					continue
				}
			}

			outChan <- reconTransaction
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

	return outChan
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

func (r *ReconService) FilterMismatched(reconTransactionChan <-chan ReconTransaction) <-chan ReconTransaction {
	return pipeline.TransformChan(reconTransactionChan, func(t ReconTransaction) (ReconTransaction, bool) {
		return t, !t.IsMatched
	})
}

func (r *ReconService) WriteToCsv(filepath string, reconTransactionChan <-chan ReconTransaction) {
	recordChan := pipeline.TransformChan(reconTransactionChan, func(t ReconTransaction) (map[string]string, bool) {
		return t.ToMap(), true
	})

	csvHeader := []string{"source", "id", "type", "amount", "date", "remark"}
	r.CsvIngester.Write(r.Ctx, filepath, csvHeader, recordChan)
}

func (r *ReconService) FilterByDate(record model.Transaction) (model.Transaction, bool) {
	if record.ParseError == nil && (r.filterDateRangeEpoch[0] > record.DateEpoch || r.filterDateRangeEpoch[1] < record.DateEpoch) {
		return record, false
	}

	return record, true
}
