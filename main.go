package main

import (
	"encoding/csv"
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

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()

	if err != nil {
		log.Fatal("unable to parse file as CSV for "+filePath, err)
	}

	return records
}

func writeInCsvFile(records [][]string, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("can't create file ", err)
	}
	defer file.Close()

	w := csv.NewWriter(file)
	err = w.WriteAll(records)

	if err != nil {
		log.Fatalln("error writing csv ", err)
	}
}

func makeTradeRecord(record []string) TradeRecord {
	userID, err := strconv.Atoi(record[0])
	if err != nil {
		log.Fatal("can't convert id's string into int ", err)
	}

	ticker := record[2]

	buyPrice, err := strconv.ParseFloat(record[3], 64)
	if err != nil {
		log.Fatal("can't convert purchase price's string into float64 ", err)
	}

	sellPrice, err := strconv.ParseFloat(record[4], 64)
	if err != nil {
		log.Fatal("can't convert selling price's string into float64 ", err)
	}

	return TradeRecord{userID, ticker, buyPrice, sellPrice}
}

func makeCandleRecord(record []string) CandleRecord {
	ticker := record[0]
	time := record[1]
	maxPrice, err := strconv.ParseFloat(record[3], 64)

	if err != nil {
		log.Fatal("can't convert max price's string into float64 ", err)
	}

	minPrice, err := strconv.ParseFloat(record[4], 64)
	if err != nil {
		log.Fatal("can't convert min price's string into float64 ", err)
	}

	return CandleRecord{ticker, time, maxPrice, minPrice}
}

func addTrade(trades map[int]map[string][]float64, record []string) {
	rec := makeTradeRecord(record)

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

func addCandle(candles map[string]PriceInfo, record []string) {
	rec := makeCandleRecord(record)

	info, ok := candles[rec.ticker]
	if !ok {
		priceInfo := PriceInfo{math.MaxFloat64, 0, "", ""}
		info = priceInfo
	}

	candles[rec.ticker] = updatePriceInfo(info, rec)
}

func parseTradesInfo(tradesInfo [][]string) map[int]map[string][]float64 {
	trades := make(map[int]map[string][]float64)

	for _, record := range tradesInfo {
		addTrade(trades, record)
	}

	return trades
}

func parseCandleInfo(candleInfo [][]string) map[string]PriceInfo {
	candles := make(map[string]PriceInfo)

	for _, record := range candleInfo {
		addCandle(candles, record)
	}

	return candles
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
	tradesInfo := readCsvFile("user_trades.csv")
	candlesInfo := readCsvFile("candles_5m.csv")
	trades := parseTradesInfo(tradesInfo)
	candles := parseCandleInfo(candlesInfo)

	records := prepareForOutput(trades, candles)
	writeInCsvFile(records, "output.csv")
}
