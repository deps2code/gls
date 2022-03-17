package gls

import "errors"

var (

	// ErrNotFound is returned when there is no record in the database for ip address
	ErrNotFound = errors.New("no record found in database")
	// ErrInvalidIP is returned when the ip address is not valid
	ErrInvalidIP = errors.New("ip address is invalid")
	// ErrInvalidIPData is returned when the ip address data is not valid
	ErrInvalidIPData = errors.New("ip address data is invalid")

	// CSV parser errors
	// ErrCsvInvalidRow is returned when the csv row has less comma seperated values than expected or error occurred in parsing the row
	ErrCsvInvalidRow = errors.New("not a valid csv row")
	// ErrCsvIPParse is returned when the ip address is not valid
	ErrCsvIPParse = errors.New("not a valid ip address")
	// ErrCsvDuplicateIP is returned when a duplicate ip address is encountered
	ErrCsvDuplicateIP = errors.New("duplicate ip address")
	// ErrCsvInvalidLat is returned when an invalid lattitude is encountered
	ErrCsvInvalidLat = errors.New("invalid lattitude")
	// ErrCsvInvalidLng is returned when an invalid longitude is encountered
	ErrCsvInvalidLng = errors.New("duplicate longitude")
	// ErrCsvInsufficientIPData is returned when a ip address does not have any data
	ErrCsvInsufficientIPData = errors.New("insufficient ip data to save")
	// ErrCSVDatabaseSave is returned when the database save operation fails
	ErrCSVDatabaseSave = errors.New("cannot save record in database")
)
