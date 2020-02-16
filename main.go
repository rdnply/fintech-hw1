package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
)

type TradeRecord struct {
	id     int
	ticker string
	buy    float64
	sell   float64
}

type CandleRecord struct {
	ticker   string
	time     string
	maxPrice float64
	minPrice float64
}

type PriceInfo struct {
	min     float64
	max     float64
	timeMin string
	timeMax string
}

func readCSVFile(filePath string) ([][]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, errors.New("unable to read input file " + filePath)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()

	if err != nil {
		return nil, errors.New("unable to parse file as CSV for " + filePath)
	}

	return records, nil
}

func writeInCSVFile(records [][]string, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return errors.New("can't create file ")
	}
	defer file.Close()

	w := csv.NewWriter(file)
	err = w.WriteAll(records)

	if err != nil {
		return errors.New("error writing csv ")
	}

	return nil
}

func makeTradeRecord(record []string) (*TradeRecord, error) {
	const (
		UserIDIndex    = 0
		TickerIndex    = 2
		BuyPriceIndex  = 3
		SellPriceIndex = 4
	)

	userID, err := strconv.Atoi(record[UserIDIndex])
	if err != nil {
		return nil, errors.New("can't convert id's string into int ")
	}

	ticker := record[TickerIndex]

	buyPrice, err := strconv.ParseFloat(record[BuyPriceIndex], 64)
	if err != nil {
		return nil, errors.New("can't convert purchase price's string into float64 ")
	}

	sellPrice, err := strconv.ParseFloat(record[SellPriceIndex], 64)
	if err != nil {
		return nil, errors.New("can't convert selling price's string into float64 ")
	}

	return &TradeRecord{userID, ticker, buyPrice, sellPrice}, nil
}

func makeCandleRecord(record []string) (*CandleRecord, error) {
	const (
		TickerIndex   = 0
		TimeIndex     = 1
		MaxPriceIndex = 3
		MinPriceIndex = 4
	)

	ticker := record[TickerIndex]
	time := record[TimeIndex]
	maxPrice, err := strconv.ParseFloat(record[MaxPriceIndex], 64)

	if err != nil {
		return nil, errors.New("can't convert max price's string into float64 ")
	}

	minPrice, err := strconv.ParseFloat(record[MinPriceIndex], 64)
	if err != nil {
		return nil, errors.New("can't convert min price's string into float64 ")
	}

	return &CandleRecord{ticker, time, maxPrice, minPrice}, nil
}

func addTrade(trades map[int]map[string][]float64, record []string) error {
	rec, err := makeTradeRecord(record)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	idInfo, ok := trades[rec.id]
	if !ok {
		trades[rec.id] = make(map[string][]float64)
		if rec.buy != 0 {
			trades[rec.id][rec.ticker] = append(trades[rec.id][rec.ticker], rec.buy)
		} else {
			trades[rec.id][rec.ticker] = append(trades[rec.id][rec.ticker], rec.sell)
		}
	} else {
		tickerInfo, ok := idInfo[rec.ticker]
		if !ok {
			if rec.buy != 0 {
				idInfo[rec.ticker] = append(idInfo[rec.ticker], rec.buy)
			} else {
				idInfo[rec.ticker] = append(idInfo[rec.ticker], rec.sell)
			}
		} else {
			if rec.buy != 0 {
				tickerInfo = append(tickerInfo, rec.buy)
			} else {
				tickerInfo = append(tickerInfo, rec.sell)
			}
			idInfo[rec.ticker] = tickerInfo
		}
	}

	return nil
}

func updatePriceInfo(old PriceInfo, newRec CandleRecord) PriceInfo {
	result := old
	if result.max < newRec.maxPrice {
		result.max = newRec.maxPrice
		result.timeMax = newRec.time
	}

	if result.min > newRec.minPrice {
		result.min = newRec.minPrice
		result.timeMin = newRec.time
	}

	return result
}

func addCandle(candles map[string]PriceInfo, record []string) error {
	rec, err := makeCandleRecord(record)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	info, ok := candles[rec.ticker]
	if !ok {
		priceInfo := PriceInfo{math.MaxFloat64, 0, "", ""}
		info = priceInfo
	}

	candles[rec.ticker] = updatePriceInfo(info, *rec)

	return nil
}

func parseTradesInfo(tradesInfo [][]string) (map[int]map[string][]float64, error) {
	trades := make(map[int]map[string][]float64)

	for _, record := range tradesInfo {
		err := addTrade(trades, record)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}
	}

	return trades, nil
}

func parseCandleInfo(candleInfo [][]string) (map[string]PriceInfo, error) {
	candles := make(map[string]PriceInfo)

	for _, record := range candleInfo {
		err := addCandle(candles, record)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}
	}

	return candles, nil
}

func makeRecord(id int, ticker string, prices []float64, priceInfo PriceInfo) []string {
	record := make([]string, 0)
	record = append(record, strconv.Itoa(id))
	record = append(record, ticker)
	userDiff := prices[1] - prices[0]
	userRevenue := strconv.FormatFloat(userDiff, 'f', 2, 64)
	record = append(record, userRevenue)
	maxDiff := priceInfo.max - priceInfo.min
	maxRevenue := strconv.FormatFloat(maxDiff, 'f', 2, 64)
	record = append(record, maxRevenue)
	diff := maxDiff - userDiff
	lost := strconv.FormatFloat(diff, 'f', 2, 64)
	record = append(record, lost)
	record = append(record, priceInfo.timeMax, priceInfo.timeMin)

	return record
}

func prepareForOutput(trades map[int]map[string][]float64, candles map[string]PriceInfo) [][]string {
	output := make([][]string, 0)

	for id, tickerInfo := range trades {
		for ticker, prices := range tickerInfo {
			record := makeRecord(id, ticker, prices, candles[ticker])
			output = append(output, record)
		}
	}

	return output
}

func main() {
	tradesInfo, err := readCSVFile("user_trades.csv")
	if err != nil {
		log.Fatal(err)
	}

	candlesInfo, err := readCSVFile("candles_5m.csv")
	if err != nil {
		log.Fatal(err)
	}

	trades, err := parseTradesInfo(tradesInfo)
	if err != nil {
		log.Fatal(err)
	}

	candles, err := parseCandleInfo(candlesInfo)
	if err != nil {
		log.Fatal(err)
	}

	records := prepareForOutput(trades, candles)

	err = writeInCSVFile(records, "output.csv")
	if err != nil {
		log.Fatal(err)
	}
}
