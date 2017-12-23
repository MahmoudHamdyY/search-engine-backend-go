package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func main() {
	router := httprouter.New()
	searcher := NewSearcher()

	router.GET("/search", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		qs := r.URL.Query()

		if _, ok := qs["q"]; !ok {
			w.WriteHeader(400)
			return
		}

		query := qs["q"][0]

		results := searcher.Search(query)

		res, err := json.Marshal(results)

		if err != nil {
			w.WriteHeader(500)
			return
		}

		w.Write(res)
	})

	log.Fatal(http.ListenAndServe(":8080", router))
}
