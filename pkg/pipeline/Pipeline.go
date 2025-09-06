package pipeline

import (
	"sync"
)

func GetTransformerChans[In any, Out any](
	inputChan <-chan In,
	outputChan chan<- Out,
	workerCount int,
	transformFunc func(In) Out,
) {
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := range workerCount {
		go func(id int) {
			defer wg.Done()
			for item := range inputChan {
				transformedItem := transformFunc(item)
				outputChan <- transformedItem
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(outputChan)
	}()
}

func TransformChan[In any, Out any](
	inputChan <-chan In,
	transformFunc func(In) (Out, bool),
) <-chan Out {
	outputChan := make(chan Out, 10)
	go func() {
		defer close(outputChan)
		for item := range inputChan {
			if transformedItem, ok := transformFunc(item); ok {
				outputChan <- transformedItem
			}
		}
	}()

	return outputChan
}

func CombineChans[T any](chans ...<-chan T) <-chan T {
	out := make(chan T)

	var wg sync.WaitGroup
	wg.Add(len(chans))

	for _, c := range chans {
		go func(c <-chan T) {
			defer wg.Done()
			for n := range c {
				out <- n
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
