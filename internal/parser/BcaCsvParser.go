package parser

import (
	"math"
	"project/internal/model"
	"strconv"

	"time"

	"github.com/mitchellh/mapstructure"
)

type BcaCsv struct {
	Id     string `mapstructure:"ext_id"`
	Amount string `mapstructure:"amount"`
	Date   string `mapstructure:"date"` // YYYY-MM-DD
}

type BcaParser struct {
}

func NewBcaParser() *BcaParser {
	return &BcaParser{}
}

func (a *BcaParser) Parse(record map[string]string) model.Transaction {
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

	txnType := "CREDIT"
	if amountf64 < 0 {
		txnType = "DEBIT"
	}

	return model.Transaction{
		Source:     "bca",
		Id:         dbsCsv.Id,
		Type:       txnType,
		Amount:     math.Abs(amountf64),
		Date:       t.Format("2006-01-02"),
		DateEpoch:  t.UnixMilli(),
		ParseError: parseErr,
	}
}
