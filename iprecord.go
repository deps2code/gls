package gls

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
)

type IPRecord struct {
	IPAddress    []byte
	Country      string
	CountryCode  string
	City         string
	Lat          float64
	Lng          float64
	MysteryValue interface{}
}

func (ip *IPRecord) Validate() error {
	if ip.Country == "" && ip.CountryCode == "" && ip.City == "" &&
		(ip.Lat == 0.0 || ip.Lng == 0.0) {
		return ErrCsvInsufficientIPData
	}
	return nil
}

func (ip *IPRecord) Save() error {
	ipDataRedis, err := json.Marshal(ip)
	if err != nil {
		fmt.Println(err)
		return ErrInvalidIPData
	}
	_, err = RedisContext.RedisDB.Set(context.Background(), string(ip.IPAddress), string(ipDataRedis), 0).Result()
	if err != nil {
		fmt.Println(err)
		return ErrCSVDatabaseSave
	}
	return nil
}

func GetData(key string) (IPRecord, error) {
	var ipGeoData IPRecord
	parsedIP := net.ParseIP(key)
	if parsedIP == nil {
		return ipGeoData, ErrInvalidIP
	}
	ipData, err := RedisContext.RedisDB.Get(context.Background(), string(parsedIP.To4())).Result()
	if err != nil {
		return ipGeoData, ErrNotFound
	}

	err = json.Unmarshal([]byte(ipData), &ipGeoData)
	if err != nil {
		return ipGeoData, ErrInvalidIPData
	}
	return ipGeoData, nil
}
