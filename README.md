# Amartha Reconciliation System

A high-performance Go application for reconciling financial transactions between internal (Amartha) and external (BCA, DBS) banking systems. The system processes CSV files, matches transactions across different sources, and identifies discrepancies.

## Features

- **Multi-source Transaction Processing**: Supports Amartha (internal), BCA, and DBS bank CSV formats
- **Intelligent Matching**: Matches transactions by ID, amount, and date
- **Date Range Filtering**: Process transactions within specific date ranges
- **Discrepancy Detection**: Identifies matched transactions amount differences
- **Comprehensive Reporting**: Generates detailed reconciliation summaries and CSV outputs
- **High Performance**: Uses Go channels and concurrent processing for efficient data handling

## Project Structure

```
amartha-recon/
├── cmd/
│   └── main.go                 # Application entry point
├── internal/
│   ├── model/                  # Transaction data model
│   ├── parser/                 # Source CSV parsers
│   └── services/               # Core logic layer
├── pkg/
│   ├── ingester/               # CSV file processing
│   │   └── CsvIngester.go      
│   ├── pipeline/               # Data pipeline utilities
│   │   └── Pipeline.go         
│   └── storage/                # Bespoke table implementation
│       ├── HashTable.go        
│       └── SearchTree.go
├── bin/                        # Sample data
└── Makefile                    # Build and test commands
```

## CSV Format Support

### Amartha (Internal) Format
```csv
id,type,amount,date
not_included_1,CREDIT,1,2024-12-30 10:00:05
no_match_1,CREDIT,1,2025-10-05 10:00:00
```

### BCA Format
```csv
ext_id,amount,date
bca_match_id_1,4,2025-01-02
```

### DBS Format
```csv
ext_id,type,amount,date
dbs_match_id_1,DEBIT,4,2025-01-01
```

## Installation

### Prerequisites
- Go 1.25.1 or later
- Git

### Setup
1. Clone the repository:
```bash
git clone https://github.com/kevin-luvian/amartha-recon.git
cd amartha-recon
```

2. Install dependencies:
```bash
go mod download
```

## Usage

### Running the Application

1. **Using Makefile** (recommended):
```bash
make run
```

2. **Direct Go command**:
```bash
go run cmd/main.go
```

### Configuration

The application is configured in `cmd/main.go`. Key settings:

- **File Paths**: Update the `FILES` map to point to your CSV files
- **Date Range**: Modify `FilterDateRange` to set the processing date range
- **Sources**: Configure which internal and external sources to process

```go
var FILES = map[string]string{
    "amartha": "/path/to/amartha.csv",
    "bca":     "/path/to/bca.csv", 
    "dbs":     "/path/to/dbs.csv",
    "output":  "/path/to/output.csv",
}
```

### Sample Output

The application generates:
1. **Console Summary**: Reconciliation statistics
2. **CSV Output**: Detailed mismatch report with remarks

Example console output:
```
====== Reconciliation Summary ======
Total Matched Transactions: 5
Total Mismatched Transactions: 3
Total Mismatches by Source:
  - amartha: 2 mismatches
  - dbs: 1 mismatches
Total Discrepancy Amount: 13.00
====================================
```

Example CSV output:
```csv
source,id,type,amount,date,remark
dbs,dbs_error_negative_1,DEBIT,-10.00,2025-01-01,negative amount provided
amartha,no_match_1,CREDIT,1.00,2025-10-05,No matching external transaction found
```

## Testing

Run the test suite with coverage:
```bash
make test
```

This will:
- Execute all unit tests
- Generate a coverage report (`coverage.out`)

## Reconciliation Logic

The system performs reconciliation in several stages:

1. **Data Ingestion**: Parse CSV files from multiple sources
2. **Date Filtering**: Filter transactions within the specified date range
3. **Matching Algorithm**: 
   - Primary match: ID-based matching
   - Secondary match: Amount and date matching
   - Error detection: Parsing error or invalid data
4. **Summary Generation**: Aggregate statistics and discrepancies
5. **Output Generation**: Export mismatched transactions to CSV

## Transaction Model

Each transaction contains:
- `Source`: Origin (amartha, bca, dbs)
- `Id`: Unique transaction identifier
- `Type`: Transaction type (CREDIT/DEBIT)
- `Amount`: Transaction amount (max 2 decimal places)
- `Date`: Transaction date (YYYY-MM-DD format)
- `DateEpoch`: Unix timestamp for efficient sorting
- `ParseError`: Any parsing errors encountered

## HashTable Implementation

The reconciliation system uses a custom HashTable that combines a hash map with a search tree for efficient transaction matching, providing both O(1) direct lookups and flexible hierarchical searching.

### Structure

**SearchTree:**
```
2025-01-01:
  DEBIT:
    5.00:
      txn_1: 2025-01-01|DEBIT|txn_1
  CREDIT:
    5.00:
      txn_2: 2025-01-01|CREDIT|txn_2
    6.00:
      txn_3: 2025-01-01|CREDIT|txn_3
```

**Table:**
```
2025-01-01|DEBIT|txn_1: &{ID: txn_1, Amount: 5.00, ...}
2025-01-01|CREDIT|txn_2: &{ID: txn_2, Amount: 5.00, ...}
2025-01-01|CREDIT|txn_3: &{ID: txn_3, Amount: 6.00, ...}
```

### Key Operations

- **Put**: Adds a transaction to both the hash table and search tree
- **GetById**: Direct O(1) lookup using the full hash key
- **GetFirstMatchByPath**: Searches using partial path (e.g., `["2025-01-01", "DEBIT"]`)
- **IsPathContainsOneValue**: Checks if a path contains exactly one transaction
- **Remove**: Removes transaction and prunes empty tree branches

This hybrid approach enables fast exact matching via hash keys and flexible partial matching through the hierarchical search tree structure.

## Performance Features

- **Concurrent Processing**: Uses Go channels for parallel data processing
- **Memory Efficient**: Streaming CSV processing without loading entire files
- **Hash-based Lookups**: Fast transaction matching using hash tables
- **Pipeline Architecture**: Modular data transformation pipeline

## License

This project is licensed under the terms specified in the LICENSE file.
