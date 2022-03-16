package gls

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
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

func ParseCSV() {
	f, err := os.Open("./data.csv")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	var IPDataList []IPRecord

	ipDuplicateMap := make(map[string]bool)

	csvReader := csv.NewReader(f)
	for {

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if len(record) < 6 {
			fmt.Println("Error occurred: not a valid csv row")
			continue
		}

		//parsing ip address
		parsedIP := net.ParseIP(record[0])
		if parsedIP == nil {
			fmt.Println("Error occurred: Invalid IP address")
			continue
		}

		if _, ok := ipDuplicateMap[record[0]]; ok {
			fmt.Println("Error occurred: Duplicate IP Address")
			continue
		}

		ipDuplicateMap[record[0]] = true

		parsedLat, err := strconv.ParseFloat(record[4], 64)
		if err != nil {
			fmt.Println("Error occurred: Invalid Lat")
			continue
		}

		parsedLng, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			fmt.Println("Error occurred: Invalid Lng")
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
			continue
		}

		IPDataList = append(IPDataList, ipData)

	}

	for _, record := range IPDataList {
		err := record.Save()
		if err != nil {
			fmt.Println("Error occurred: " + err.Error())
		}
	}

}
