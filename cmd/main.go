package main

import (
	"context"
	"fmt"
	"project/internal/parser"
	"project/internal/services"
	"project/pkg/ingester"
	"project/pkg/pipeline"
)

var FILES = map[string]string{
	"amartha": "/home/kevinluvianh/Documents/amartha-go/bin/amartha_sample.csv",
	"bca":     "/home/kevinluvianh/Documents/amartha-go/bin/bca_sample.csv",
	"dbs":     "/home/kevinluvianh/Documents/amartha-go/bin/dbs_sample.csv",
	"output":  "/home/kevinluvianh/Documents/amartha-go/bin/out_sample.csv",
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reconSummary := services.NewReconSummary()
	reconService := services.NewReconService(services.NewReconServiceOpts{
		Ctx:             ctx,
		CsvIngester:     ingester.NewCsvIngester(),
		FilterDateRange: []string{"2025-01-01", "2026-01-01"},
	})

	internalTransactionChan := reconService.ReadInternalCsv(services.ReconCsvDetail{
		Source:      "amartha",
		CsvFilepath: FILES["amartha"],
		Parser:      parser.NewAmarthaParser(),
	})
	dbsTransactionChan := reconService.ReadExternalCsv(services.ReconCsvDetail{
		Source:      "dbs",
		CsvFilepath: FILES["dbs"],
		Parser:      parser.NewDbsParser(),
	})
	bcaTransactionChan := reconService.ReadExternalCsv(services.ReconCsvDetail{
		Source:      "bca",
		CsvFilepath: FILES["bca"],
		Parser:      parser.NewBcaParser(),
	})

	transactionChan := pipeline.CombineChans(internalTransactionChan, dbsTransactionChan, bcaTransactionChan)
	transactionChan = pipeline.TransformChan(transactionChan, reconService.FilterByDate)
	reconTransactionChan := reconService.Reconcile(transactionChan)
	reconTransactionChan = reconService.PassThroughSummary(reconTransactionChan, reconSummary)
	mismatchedChan := reconService.FilterMismatched(reconTransactionChan)
	reconService.WriteToCsv(FILES["output"], mismatchedChan)

	fmt.Println("====== Reconciliation Summary ======")
	fmt.Printf("Total Matched Transactions: %d\n", reconSummary.TotalMatched)
	fmt.Printf("Total Mismatched Transactions: %d\n", reconSummary.TotalMismatched)
	fmt.Printf("Total Mismatches by Source:\n")
	for source, count := range reconSummary.TotalMismatchBySource {
		fmt.Printf("  - %s: %d mismatches\n", source, count)
	}
	fmt.Printf("Total Discrepancy Amount: %.2f\n", reconSummary.TotalDiscrepancy)
	fmt.Println("====================================")
}
