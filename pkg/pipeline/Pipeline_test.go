package pipeline

import (
	"testing"
)

func TestGetTransformerChans(t *testing.T) {
	input := make(chan int, 5)
	output := make(chan int, 5)

	for i := range 5 {
		input <- i
	}

	close(input)

	transform := func(x int) int { return x * 2 }
	GetTransformerChans(input, output, 2, transform)

	results := []int{}

	for v := range output {
		results = append(results, v)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	for _, v := range results {
		if v%2 != 0 {
			t.Errorf("Expected even number, got %d", v)
		}
	}
}

func TestTransformChan(t *testing.T) {
	input := make(chan int, 5)
	for i := range 5 {
		input <- i
	}
	close(input)

	transform := func(x int) (int, bool) {
		if x%2 == 0 {
			return x * 10, true
		}
		return 0, false
	}

	output := TransformChan(input, transform)
	results := []int{}

	for v := range output {
		results = append(results, v)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	for _, v := range results {
		if v%10 != 0 {
			t.Errorf("Expected multiple of 10, got %d", v)
		}
	}
}

func TestCombineChans(t *testing.T) {
	ch1 := make(chan int, 2)
	ch2 := make(chan int, 2)
	ch1 <- 1
	ch1 <- 2
	ch2 <- 3
	ch2 <- 4
	close(ch1)
	close(ch2)

	combined := CombineChans(ch1, ch2)
	results := []int{}

	for v := range combined {
		results = append(results, v)
	}

	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	m := map[int]bool{}

	for _, v := range results {
		m[v] = true
	}

	for i := 1; i <= 4; i++ {
		if !m[i] {
			t.Errorf("Missing value %d in results", i)
		}
	}
}
