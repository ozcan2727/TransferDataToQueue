package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

var db *sql.DB
var connStr = "postgresql://" + getEnv("DB_USER_NAME") + ":" + getEnv("DB_PASSWORD") + "@" + getEnv("DB_URL") + "/" + getEnv("DB_NAME") + "?sslmode=disable"

type ChangeEventPatch struct {
	ObjectName string `json:"object_name,omitempty"`
	ObjectID   string `json:"object_id,omitempty"`
	EventType  string `json:"event_type"`
}

func getEnv(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		fmt.Printf("%s not set\n", key)
	} else {
		return val
	}
	return ""
}

func init() {
	var err error

	db, err = sql.Open("postgres", connStr)

	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
	// this will be printed in the terminal, confirming the connection to the database
	fmt.Println("The database is connected")
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
func failOnError(err error, msg string) {

	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func GetAuthor(query string) {
	var err error
	if err = db.Ping(); err != nil {
		panic(err)
		panic(err)
	}
	queryParams := "SELECT num FROM (VALUES" + query + ")" + " AS t (num)  except " + " select id from tbl_author;"
	rows, err := db.Query(queryParams)
	CheckError(err)

	var authorIds []int
	for rows.Next() {
		var authorId int
		err = rows.Scan(&authorId)
		CheckError(err)
		SendChangeEventPatch(authorId)
		authorIds = append(authorIds, authorId)
	}
	rows.Close()
	if len(authorIds) > 0 {
		fmt.Print(",")
		for i, authorId := range authorIds {
			fmt.Print(" ", authorId)
			if i < len(authorIds)-1 {
				fmt.Print(",")
			}
		}
	} else {
		fmt.Println("All provided author IDs were found.")
	}
}

func SendChangeEventPatch(id int) {
	var conn, err = amqp.Dial("amqp://" + getEnv("CHANGE_MQ_PASSWORD") + ":" + getEnv("CHANGE_MQ_USER") + "@" + getEnv("CHANGE_MQ_URL") + "/")
	failOnError(err, "Failed to connect to Notification RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	event := ChangeEventPatch{
		ObjectName: "Author",
		ObjectID:   strconv.Itoa(id),
		EventType:  "DELETE",
	}
	body, err := json.Marshal(event)
	failOnError(err, "Failed to marshal event data")

	queueName := "change_event_patch"

	err = ch.Publish("", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	failOnError(err, "Failed to publish message")
}

func SolrQuering(counter int) {
	client := &http.Client{}
	baseURL := "http://solr9.trdizinprod.svc.cluster.ulakbim:8983/solr/author_0625/select?"
	rows := 1000
	var query = ""
	var querycounter = 1
	for {
		queryParams := fmt.Sprintf("fl=id&indent=true&q.op=OR&q=id%%3A*&start=%d&rows=%d&sort=id%%20asc", counter, rows)
		fullURL := baseURL + queryParams
		req, err := http.NewRequest("GET", fullURL, nil)
		if err != nil {
			fmt.Printf("error making http request: %s\n", err)
		}

		req.Header.Add("Accept", "application/vnd.orcid+json")
		res, err := client.Do(req)
		if err != nil {
			fmt.Printf("error making http request: %s\n", err)
		}
		defer res.Body.Close()
		if err != nil {
			fmt.Println("No response from request")
		}
		body, err := ioutil.ReadAll(res.Body) // response body is []byte
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(body), &data); err != nil {
			fmt.Println("JSON parse hatası:", err)
			return
		}

		responseData, ok := data["response"].(map[string]interface{})
		if !ok {
			fmt.Println("response bulunamadı veya uygun değil")
			return
		}
		docs, ok := responseData["docs"].([]interface{})
		if !ok {
			fmt.Println("docs bulunamadı veya uygun değil")
			return
		}
		for _, doc := range docs {
			docMap, ok := doc.(map[string]interface{})
			if !ok {
				fmt.Println("doc uygun değil")
				continue
			}
			id, idOk := docMap["id"].(string)
			if !idOk {
				fmt.Println("id uygun değil")
				continue
			}
			if querycounter%1000 == 0 {
				querycounter += 1
				query += "(" + id + ")"
			} else {
				querycounter += 1
				query += "(" + id + "),"
			}
		}
		GetAuthor(query)
		query = ""
		counter += rows
	}
}

func main() {
	counter := 0

	SolrQuering(counter)
	err := db.Close()
	if err != nil {
		fmt.Printf("Error closing the database connection: %s\n", err)
	}

}
