package tcc

import (
	"database/sql"
	"fmt"
	"sync"
)

var (
	dOnce sync.Once
	db    *sql.DB
)

// InitDB initializes the database connection
func InitDB(dsn string) (*sql.DB, error) {
	// dsn: "username:password@tcp(127.0.0.1:3306)/dbname"
	var err error
	dOnce.Do(func() {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			fmt.Println("init db failed, err:", err)
			return
		}
	})
	return db, err
}
