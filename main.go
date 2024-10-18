package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type SyncData struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

var db *sql.DB
var maxSyncedID int

func main() {
	var err error
	// 连接数据库
	db, err = sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/dbname")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 初始化最大同步ID
	maxSyncedID = getMaxSyncedID()

	// 每5分钟执行一次同步
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		syncData()
	}
}

func getMaxSyncedID() int {
	var maxID int
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM synced_data").Scan(&maxID)
	if err != nil {
		log.Printf("Error getting max synced ID: %v", err)
		return 0
	}
	return maxID
}

func syncData() {
	url := fmt.Sprintf("http://system-a-api.com/api/data?since_id=%d", maxSyncedID)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error calling API: %v", err)
		return
	}
	defer resp.Body.Close()

	var data []SyncData
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Printf("Error decoding response: %v", err)
		return
	}

	for _, item := range data {
		if err := insertData(item); err != nil {
			log.Printf("Error inserting data: %v", err)
			continue
		}
		if item.ID > maxSyncedID {
			maxSyncedID = item.ID
		}
	}

	log.Printf("Sync completed. New max synced ID: %d", maxSyncedID)
}

func insertData(item SyncData) error {
	_, err := db.Exec("INSERT INTO synced_data (id, data) VALUES (?, ?)", item.ID, item.Data)
	return err
}