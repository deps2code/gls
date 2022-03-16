package gls

import (
	"context"
	"encoding/json"
	"errors"
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
		return errors.New("inconclusive data")
	}
	return nil
}

func (ip *IPRecord) Save() error {
	ipDataRedis, err := json.Marshal(ip)
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = RedisContext.RedisDB.Set(context.Background(), string(ip.IPAddress), string(ipDataRedis), 0).Result()
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func GetData(key string) (IPRecord, error) {
	var ipGeoData IPRecord
	parsedIP := net.ParseIP(key)
	if parsedIP == nil {
		return ipGeoData, errors.New("invalid ip address")
	}
	ipData, err := RedisContext.RedisDB.Get(context.Background(), string(parsedIP.To4())).Result()
	if err != nil {
		return ipGeoData, err
	}

	err = json.Unmarshal([]byte(ipData), &ipGeoData)
	return ipGeoData, err
}
