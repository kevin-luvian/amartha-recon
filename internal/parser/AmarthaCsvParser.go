package parser

import (
	"strconv"

	"github.com/kevin-luvian/amartha-recon/internal/model"

	"time"

	"github.com/mitchellh/mapstructure"
)

type AmarthaCsv struct {
	Id     string `mapstructure:"id"`
	Type   string `mapstructure:"type"`
	Amount string `mapstructure:"amount"`
	Date   string `mapstructure:"date"` // YYYY-MM-DD HH:MM:SSZ
}

type AmarthaParser struct {
}

func NewAmarthaParser() *AmarthaParser {
	return &AmarthaParser{}
}

func (a *AmarthaParser) Parse(record map[string]string) model.Transaction {
	var amarthaCsv AmarthaCsv
	var parseErr error

	if err := mapstructure.Decode(record, &amarthaCsv); err != nil {
		parseErr = err
	}

	t, err := time.Parse(time.DateTime, amarthaCsv.Date)
	if err != nil {
		parseErr = err
	}

	tMidnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	amountf64, err := strconv.ParseFloat(amarthaCsv.Amount, 64)
	if err != nil {
		parseErr = err
	}

	return model.Transaction{
		Source:     "amartha",
		Id:         amarthaCsv.Id,
		Type:       amarthaCsv.Type,
		Amount:     amountf64,
		Date:       tMidnight.Format("2006-01-02"),
		DateEpoch:  tMidnight.UnixMilli(),
		ParseError: parseErr,
	}
}
