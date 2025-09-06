// internal/ingester/CsvIngester_test.go
package ingester

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCsvIngester_Read(t *testing.T) {
	// Create a temporary CSV file
	ctx := context.Background()
	dir := os.TempDir()
	filePath := filepath.Join(dir, "test.csv")
	content := "name,age,id\nJhon,30,\nBob,25,1\n"

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create temp csv file: %v", err)
	}
	defer os.Remove(filePath)

	ingester := NewCsvIngester()
	recordsChan, err := ingester.Read(ctx, filePath)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}

	var records []map[string]string
	for record := range recordsChan {
		records = append(records, record)
	}

	expected := []map[string]string{
		{"name": "Jhon", "age": "30"},
		{"name": "Bob", "age": "25"},
	}

	if len(records) != len(expected) {
		t.Fatalf("expected %d records, got %d", len(expected), len(records))
	}

	for i, record := range records {
		for k, v := range expected[i] {
			if record[k] != v {
				t.Errorf("expected record %d field %s to be %v, got %v", i, k, v, record[k])
			}
		}
	}
}

func TestCsvIngester_Write(t *testing.T) {
	ctx := context.Background()
	dir := os.TempDir()
	filePath := filepath.Join(dir, "test_write.csv")
	defer os.Remove(filePath)

	header := []string{"id", "name", "age"}
	recordsArr := []map[string]string{
		{"name": "Alice", "age": "22", "id": "100"},
		{"name": "Bob", "age": "25", "id": "101"},
	}
	recordsChan := make(chan map[string]string, len(recordsArr))
	for _, r := range recordsArr {
		recordsChan <- r
	}
	close(recordsChan)

	ingester := NewCsvIngester()
	err := ingester.Write(ctx, filePath, header, recordsChan)
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	// Read back the file to verify contents
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read written file: %v", err)
	}

	content := string(data)
	expected := "id,name,age\n100,Alice,22\n101,Bob,25\n"
	if content != expected {
		t.Errorf("Expected %s, Got %s", expected, content)
	}
}
