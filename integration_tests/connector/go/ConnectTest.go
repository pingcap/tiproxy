package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
	"os"
)

func main() {

	host := os.Args[1]
	user := os.Args[2]
	password := os.Args[3]

	mysql.RegisterTLSConfig("tidb", &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: host,
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:4000)/test?tls=tidb", user, password, host))
	if err != nil {
		log.Fatal("failed to connect database", err)
	}
	defer db.Close()

	var dbName string
	err = db.QueryRow("SELECT DATABASE();").Scan(&dbName)
	if err != nil {
		log.Fatal("failed to execute query", err)
	}
	fmt.Println(dbName)
}
