package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/hrystynaa/lab4-go/datastore"
	"github.com/hrystynaa/lab4-go/httptools"
	"github.com/hrystynaa/lab4-go/signal"
)

var port = flag.Int("port", 8083, "server port")

type Request struct {
	Value string `json:"value"`
}

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()
	h := new(http.ServeMux)

	dir, err := ioutil.TempDir("", "temp-dir")
	if err != nil {
		log.Fatal(err)
	}
	db, err := datastore.NewDb(dir, 500)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	h.HandleFunc("/db/", func(rw http.ResponseWriter, req *http.Request) {
		key := req.URL.Path[len("/db/"):]

		switch req.Method {
		case http.MethodGet:
			value, err := db.Get(key)
			if err != nil {
				rw.WriteHeader(http.StatusNotFound)
				return
			}
			// resp := Response{
			// 	Key:   key,
			// 	Value: value,
			// }
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(rw).Encode(Response{
				Key:   key,
				Value: value,
			})

		case http.MethodPost:
			var body Request
			err := json.NewDecoder(req.Body).Decode(&body)
			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			err = db.Put(key, body.Value)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusCreated)

		default:
			rw.WriteHeader(http.StatusBadRequest)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
