package gls

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/spf13/viper"
)

func init() {
	viper.AddConfigPath(".")
	viper.SetConfigName("databaseConfig")
	viper.SetConfigType("json")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("Failed to initialise package, config file missing")
	}
	InitRedisDB()
}

type GLSCSVParser struct {
	FilePath  string // file path to read the csv
	Analytics ParserAnalytics
}

type ParserAnalytics struct {
	TotalRecords    int64
	RecordsParsed   int64
	RecordsRejected int64
	ErrorCountMap   map[error]int64 // map to keep different error counts
}

// internal function to get an initialized error count map with key as error and value 0
func getInitializedErrorCountMap() map[error]int64 {
	errorCountMap := make(map[error]int64)
	errorCountMap[ErrCsvInvalidRow] = 0
	errorCountMap[ErrCsvIPParse] = 0
	errorCountMap[ErrCsvDuplicateIP] = 0
	errorCountMap[ErrCsvInvalidLat] = 0
	errorCountMap[ErrCsvInvalidLng] = 0
	errorCountMap[ErrCsvInsufficientIPData] = 0
	errorCountMap[ErrCSVDatabaseSave] = 0

	return errorCountMap
}

func NewParser(filePath string) *GLSCSVParser {
	return &GLSCSVParser{
		FilePath: filePath,
		Analytics: ParserAnalytics{
			ErrorCountMap: getInitializedErrorCountMap(),
		},
	}
}

func (parser *GLSCSVParser) ParseCSV() error {
	f, err := os.Open(parser.FilePath)
	if err != nil {
		return err
	}

	defer f.Close()

	var IPDataList []IPRecord
	var totalRecords, recordsParsed, recordsRejected int64

	ipDuplicateMap := make(map[string]bool)

	csvReader := csv.NewReader(f)
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		totalRecords = totalRecords + 1

		// ignoring the csv header row
		if totalRecords == 1 {
			continue
		}

		if err != nil {
			fmt.Println("Error occurred: not a valid csv row")
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidRow] += 1
			continue
		}

		if len(record) < 6 {
			fmt.Println("Error occurred: not a valid csv row")
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidRow] += 1
			continue
		}

		//parsing ip address
		parsedIP := net.ParseIP(record[0])
		if parsedIP == nil {
			fmt.Println("Error occurred: Invalid IP address")
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvIPParse] += 1
			continue
		}

		if _, ok := ipDuplicateMap[record[0]]; ok {
			fmt.Println("Error occurred: Duplicate IP Address")
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvDuplicateIP] += 1
			continue
		}

		ipDuplicateMap[record[0]] = true

		parsedLat, err := strconv.ParseFloat(record[4], 64)
		if err != nil {
			fmt.Println("Error occurred: Invalid Lat")
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidLat] += 1
			continue
		}

		parsedLng, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			fmt.Println("Error occurred: Invalid Lng")
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidLng] += 1
			continue
		}

		ipData := IPRecord{
			IPAddress:    parsedIP.To4(),
			Lat:          parsedLat,
			Lng:          parsedLng,
			CountryCode:  record[1],
			Country:      record[2],
			City:         record[3],
			MysteryValue: record[6],
		}

		err = ipData.Validate()
		if err != nil {
			fmt.Println("Error occurred: " + err.Error())
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInsufficientIPData] += 1
			continue
		}

		IPDataList = append(IPDataList, ipData)

	}

	for _, record := range IPDataList {
		err := record.Save()
		if err != nil {
			fmt.Println("Error occurred: " + err.Error())
			recordsRejected = recordsRejected + 1
			parser.Analytics.ErrorCountMap[ErrCSVDatabaseSave] += 1
			continue
		}
		recordsParsed += 1
	}

	// subtracting 1 for initial header row
	parser.Analytics.TotalRecords = totalRecords - 1
	parser.Analytics.RecordsParsed = recordsParsed
	parser.Analytics.RecordsRejected = recordsRejected

	return nil
}
