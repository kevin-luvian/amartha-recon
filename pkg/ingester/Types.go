package ingester

import "context"

type ICsvIngester interface {
	Read(ctx context.Context, filepath string) (<-chan map[string]string, error)
	Write(ctx context.Context, filepath string, header []string, recordsChan <-chan map[string]string) error
}
