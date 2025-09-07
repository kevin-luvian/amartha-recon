package model

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTransaction_Hash(t *testing.T) {
	txn := Transaction{
		Id:     "txn_1",
		Source: "source",
		Type:   "type",
		Date:   "2025-01-01",
	}
	key, searchKeys := txn.Hash()

	expectedKey := fmt.Sprintf("%s|%s|%s", txn.Date, txn.Type, txn.Id)
	if key != expectedKey {
		t.Fatalf("Expected key %s, got %s", expectedKey, key)
	}

	expectedSearchKey := []string{txn.Date, txn.Type, fmt.Sprintf("%.2f", txn.Amount), txn.Id}
	if !reflect.DeepEqual(expectedSearchKey, searchKeys) {
		t.Fatalf("Expected search keys %v, got %v", expectedSearchKey, searchKeys)
	}
}

func TestTransaction_GetKeySearchByDate(t *testing.T) {
	txn := Transaction{
		Id:     "txn_1",
		Source: "source",
		Type:   "type",
		Date:   "2025-01-01",
	}
	searchKeys := txn.GetKeySearchByDate()
	expectedSearchKey := []string{txn.Date, txn.Type}
	if !reflect.DeepEqual(expectedSearchKey, searchKeys) {
		t.Fatalf("Expected search keys %v, got %v", expectedSearchKey, searchKeys)
	}
}

func TestTransaction_GetKeySearchByAmount(t *testing.T) {
	txn := Transaction{
		Id:     "txn_1",
		Source: "source",
		Type:   "type",
		Date:   "2025-01-01",
	}
	searchKeys := txn.GetKeySearchByAmount()

	expectedSearchKey := []string{txn.Date, txn.Type, fmt.Sprintf("%.2f", txn.Amount)}
	if !reflect.DeepEqual(expectedSearchKey, searchKeys) {
		t.Fatalf("Expected search keys %v, got %v", expectedSearchKey, searchKeys)
	}
}
