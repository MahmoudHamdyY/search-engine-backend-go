package main

import (
	"database/sql"
	"fmt"
	"log"
	"sort"

	_ "github.com/go-sql-driver/mysql"
	porterstemmer "github.com/reiver/go-porterstemmer"
)

type doc struct {
	name, url, summary string
}

type pair struct {
	Freq, Document_ID int
}

type so []pair

func (c so) Len() int           { return len(c) }
func (c so) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c so) Less(i, j int) bool { return c[i].Freq > c[j].Freq }

type Searcher struct {
	words   map[string]int
	files   map[int]doc
	indexes map[int][]pair
	db      *sql.DB
}

type Result struct {
	URL   string `json:"url"`
	Title string `json:"title"`
	Body  string `json:"body"`
}

func NewSearcher() *Searcher {
	searcher := &Searcher{}

	var err error

	searcher.db, err = sql.Open("mysql", "root:root@tcp(127.0.0.1)/mini_google")

	if err != nil {
		log.Println("Couldn't connect to MySQL DB")
		log.Panic(err)
	}

	searcher.load()

	return searcher
}

func (s *Searcher) Search(query string) []Result {
	stemmedQuery := porterstemmer.StemString(query)
	ret := make([]Result, 0)

	arr := s.indexes[s.words[stemmedQuery]]
	sort.Sort(so(arr))
	f := s.indexes[s.words[stemmedQuery]]

	for _, ff := range f {
		doc := s.files[ff.Document_ID]
		ret = append(ret, Result{URL: doc.url, Title: doc.name, Body: doc.summary})
		fmt.Println("Title", doc.name, "\n summary", doc.summary)
	}

	return ret
}

func (s *Searcher) load() {
	s.loadWords()
	s.loadFiles()
	s.loadIndexes()
}

func (s *Searcher) loadWords() {
	s.words = make(map[string]int)
	rows, err := s.db.Query("select * from words")
	if err != nil {
		fmt.Print(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ID int
		var Name string
		err := rows.Scan(&ID, &Name)
		if err != nil {
			fmt.Print(err)
		}
		s.words[Name] = ID
	}
	err = rows.Err()
	if err != nil {
		fmt.Print(err)
	}
}

func (s *Searcher) loadFiles() {
	s.files = make(map[int]doc)

	rows, err := s.db.Query("select * from documents")

	if err != nil {
		fmt.Print(err)
	}
	defer rows.Close()

	var ID int
	var Title, Url, Summary string

	for rows.Next() {
		err := rows.Scan(&ID, &Title, &Url, &Summary)
		if err != nil {
			fmt.Print(err)
		} else {
			s.files[ID] = doc{Title, Url, Summary}
		}
	}

	err = rows.Err()

	if err != nil {
		fmt.Print(err)
	}
}

func (s *Searcher) loadIndexes() {
	s.indexes = make(map[int][]pair)

	rows, err := s.db.Query("select * from words_documents")

	if err != nil {
		fmt.Print(err)
	}

	defer rows.Close()

	for rows.Next() {
		var Word_ID, Document_ID, Freq int
		err := rows.Scan(&Word_ID, &Document_ID, &Freq)
		if err != nil {
			fmt.Print(err)
		}
		s.indexes[Word_ID] = append(s.indexes[Word_ID], pair{Freq, Document_ID})
	}
	err = rows.Err()
	if err != nil {
		fmt.Print(err)
	}
}
