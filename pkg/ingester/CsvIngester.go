package ingester

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type CsvIngester struct {
}

func NewCsvIngester() *CsvIngester {
	return &CsvIngester{}
}

func (c *CsvIngester) Read(ctx context.Context, filepath string) (<-chan map[string]string, error) {
	recordsChan := make(chan map[string]string)

	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	go func() {
		defer file.Close()
		defer close(recordsChan)

		header := []string{}
		reader := csv.NewReader(file)

		for {
			select {
			case <-ctx.Done():
				fmt.Println("CSV reading cancelled")
				return

			default:
				record, err := reader.Read()
				if err == io.EOF {
					return
				}

				if err != nil {
					fmt.Printf("Error reading CSV record: %v\n", err)
					return
				}

				if len(header) == 0 {
					header = record
					continue
				}

				// malformed csv
				if len(record) != len(header) {
					fmt.Printf("Malformed csv record: %v\n", record)
					return
				}

				// Convert record to object
				obj := make(map[string]string, len(header))
				for i, label := range header {
					obj[label] = record[i]
				}

				recordsChan <- obj
			}
		}
	}()

	return recordsChan, nil
}

func (c *CsvIngester) Write(ctx context.Context, filepath string, header []string, recordsChan <-chan map[string]string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headerMap := make(map[string]int, len(header))
	for i, key := range header {
		headerMap[key] = i
	}

	if err := writer.Write(header); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("CSV writing cancelled")
			return nil

		case record, ok := <-recordsChan:
			if !ok {
				return nil
			}

			row := make([]string, len(headerMap))
			for key, value := range record {
				i, ok := headerMap[key]
				if ok {
					row[i] = value
				}
			}

			if err := writer.Write(row); err != nil {
				fmt.Printf("Error writing CSV record: %v\n", err)
			}
		}
	}
}
