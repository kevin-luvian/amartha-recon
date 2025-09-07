package main

import (
	"context"
	"fmt"

	"github.com/kevin-luvian/amartha-recon/internal/parser"
	"github.com/kevin-luvian/amartha-recon/internal/services"
	"github.com/kevin-luvian/amartha-recon/pkg/ingester"
	"github.com/kevin-luvian/amartha-recon/pkg/pipeline"
)

var FILES = map[string]string{
	"amartha": "/home/kevinluvianh/Documents/amartha-recon/bin/amartha_sample.csv",
	"bca":     "/home/kevinluvianh/Documents/amartha-recon/bin/bca_sample.csv",
	"dbs":     "/home/kevinluvianh/Documents/amartha-recon/bin/dbs_sample.csv",
	"output":  "/home/kevinluvianh/Documents/amartha-recon/bin/out_sample.csv",
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reconSummary := services.NewReconSummary()
	reconService, err := services.NewReconService(services.NewReconServiceOpts{
		Ctx:             ctx,
		CsvIngester:     ingester.NewCsvIngester(),
		FilterDateRange: []string{"2025-01-01", "2026-01-01"},
	})
	if err != nil {
		panic(err)
	}

	internalTransactionChan, err := reconService.ReadInternalCsv(services.ReconCsvDetail{
		Source:      "amartha",
		CsvFilepath: FILES["amartha"],
		Parser:      parser.NewAmarthaParser(),
	})
	if err != nil {
		panic(err)
	}

	dbsTransactionChan, err := reconService.ReadExternalCsv(services.ReconCsvDetail{
		Source:      "dbs",
		CsvFilepath: FILES["dbs"],
		Parser:      parser.NewDbsParser(),
	})
	if err != nil {
		panic(err)
	}

	bcaTransactionChan, err := reconService.ReadExternalCsv(services.ReconCsvDetail{
		Source:      "bca",
		CsvFilepath: FILES["bca"],
		Parser:      parser.NewBcaParser(),
	})
	if err != nil {
		panic(err)
	}

	transactionChan := pipeline.CombineChans(internalTransactionChan, dbsTransactionChan, bcaTransactionChan)
	transactionChan = pipeline.TransformChan(transactionChan, reconService.FilterByDate)
	reconTransactionChan, err := reconService.Reconcile(transactionChan)
	if err != nil {
		panic(err)
	}

	reconTransactionChan = reconService.PassThroughSummary(reconTransactionChan, reconSummary)
	mismatchedChan := pipeline.TransformChan(reconTransactionChan, reconService.FilterMismatched)
	err = reconService.WriteToCsv(FILES["output"], mismatchedChan)
	if err != nil {
		panic(err)
	}

	fmt.Println("====== Reconciliation Summary ======")
	fmt.Printf("Total Processed Transactions: %d\n", reconSummary.TotalMatched+reconSummary.TotalMismatched)
	fmt.Printf("Total Matched Transactions: %d\n", reconSummary.TotalMatched)
	fmt.Printf("Total Mismatched Transactions: %d\n", reconSummary.TotalMismatched)
	fmt.Printf("Total Mismatches by Source:\n")
	for source, count := range reconSummary.TotalMismatchBySource {
		fmt.Printf("  - %s: %d mismatches\n", source, count)
	}
	fmt.Printf("Total Discrepancy Amount: %.2f\n", reconSummary.TotalDiscrepancy)
	fmt.Println("====================================")
}
