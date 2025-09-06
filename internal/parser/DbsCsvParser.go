package parser

import (
	"fmt"
	"project/internal/model"
	"strconv"

	"time"

	"github.com/mitchellh/mapstructure"
)

type DbsCsv struct {
	Id     string `mapstructure:"ext_id"`
	Type   string `mapstructure:"type"`
	Amount string `mapstructure:"amount"`
	Date   string `mapstructure:"date"` // YYYY-MM-DD
}

type DbsParser struct {
}

func NewDbsParser() *DbsParser {
	return &DbsParser{}
}

func (a *DbsParser) Parse(record map[string]string) model.Transaction {
	var dbsCsv DbsCsv
	var parseErr error

	if err := mapstructure.Decode(record, &dbsCsv); err != nil {
		parseErr = err
	}

	t, err := time.Parse(time.DateOnly, dbsCsv.Date)
	if err != nil {
		parseErr = err
	}

	amountf64, err := strconv.ParseFloat(dbsCsv.Amount, 64)
	if err != nil {
		parseErr = err
	}

	if amountf64 < 0 {
		parseErr = fmt.Errorf("negative amount provided")
	}

	return model.Transaction{
		Source:     "dbs",
		Id:         dbsCsv.Id,
		Type:       dbsCsv.Type,
		Amount:     amountf64,
		Date:       t.Format("2006-01-02"),
		DateEpoch:  t.UnixMilli(),
		ParseError: parseErr,
	}
}
