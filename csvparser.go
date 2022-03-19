package gls

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

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
	err = InitRedisDB()
	if err != nil {
		fmt.Println("Failed to initialize redis, err:", err)
	}
}

type GLSCSVParser struct {
	NoOfThreads int64
	FilePath    string // file path to read the csv
	Analytics   ParserAnalytics
}

type ParserAnalytics struct {
	TimeTaken       time.Duration
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

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func NewParser(filePath string, noOfThreads int64) *GLSCSVParser {
	return &GLSCSVParser{
		NoOfThreads: noOfThreads,
		FilePath:    filePath,
		Analytics: ParserAnalytics{
			ErrorCountMap: getInitializedErrorCountMap(),
		},
	}
}

func (parser *GLSCSVParser) ParseCSV() error {
	start := time.Now()
	f, err := os.Open(parser.FilePath)
	if err != nil {
		return err
	}

	defer f.Close()

	var IPDataList []IPRecord
	var totalRecords, recordsRejected int64

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
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidRow] += 1
			continue
		}

		if len(record) < 6 {
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidRow] += 1
			continue
		}

		// parsing ip address
		parsedIP := net.ParseIP(record[0])
		if parsedIP == nil {
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvIPParse] += 1
			continue
		}

		// checking for duplicate IPs in map
		if _, ok := ipDuplicateMap[record[0]]; ok {
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvDuplicateIP] += 1
			continue
		}

		ipDuplicateMap[record[0]] = true

		parsedLat, err := strconv.ParseFloat(record[4], 64)
		if err != nil {
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidLat] += 1
			continue
		}
		if parsedLat > 90 || parsedLat < -90 {
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidLat] += 1
			continue
		}

		parsedLng, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInvalidLng] += 1
			continue
		}
		if parsedLng > 180 || parsedLng < -180 {
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
			recordsRejected += 1
			parser.Analytics.ErrorCountMap[ErrCsvInsufficientIPData] += 1
			continue
		}

		IPDataList = append(IPDataList, ipData)

	}

	noOfThreads := min(parser.NoOfThreads, int64(len(IPDataList)))
	chunksLength := int64(len(IPDataList)) / noOfThreads
	i := int64(0)

	ch := make(chan int64, noOfThreads+1)
	var wg sync.WaitGroup

	for {
		if i >= int64(len(IPDataList)) {
			break
		}
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			recordsRejected := int64(0)
			for _, record := range IPDataList[i:min(i+chunksLength, int64(len(IPDataList)))] {
				err := record.Save()
				if err != nil {
					recordsRejected = recordsRejected + 1
					continue
				}
			}
			ch <- recordsRejected
		}(i)
		i += chunksLength
	}

	wg.Wait()
	close(ch)

	recordsRejectedRedisSave := int64(0)
	for i := range ch {
		recordsRejectedRedisSave += i
	}

	parser.Analytics.ErrorCountMap[ErrCSVDatabaseSave] += recordsRejectedRedisSave

	// subtracting 1 from total records for initial header row
	parser.Analytics.TotalRecords = totalRecords - 1
	parser.Analytics.RecordsRejected = recordsRejected + recordsRejectedRedisSave
	parser.Analytics.RecordsParsed = parser.Analytics.TotalRecords - parser.Analytics.RecordsRejected
	parser.Analytics.TimeTaken = time.Since(start)

	return nil
}
