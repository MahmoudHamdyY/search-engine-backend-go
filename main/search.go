package main

import (
	"database/sql"
	"log"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	porterstemmer "github.com/reiver/go-porterstemmer"
)

type doc struct {
	ID                 int
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
	vis     map[int]bool
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
		go log.Println("Couldn't connect to MySQL DB")
		log.Panic(err)
	}

	searcher.load()

	return searcher
}

func (s *Searcher) Search(query string) []Result {
	stemmedQuery := porterstemmer.StemString(query)
	//ret := make([]Result, 0)
	ret := s.Search_File_Name(query)

	arr := s.indexes[s.words[stemmedQuery]]
	sort.Sort(so(arr))
	f := s.indexes[s.words[stemmedQuery]]

	for _, ff := range f {
		if !s.vis[ff.Document_ID] {
			doc := s.files[ff.Document_ID]
			ret = append(ret, Result{URL: doc.url, Title: doc.name, Body: doc.summary})
		}
		s.vis[ff.Document_ID] = true
	}

	return ret
}

func (s *Searcher) Search_File_Name(query string) []Result {
	s.vis = make(map[int]bool)
	stemmedQuery := porterstemmer.StemString(query)
	ret := make([]Result, 0)
	for _, ff := range s.files {
		title := ff.name
		if Contain(stemmedQuery, title) {
			s.vis[ff.ID] = true
			ret = append(ret, Result{URL: ff.url, Title: ff.name, Body: ff.summary})
		}
	}

	return ret
}

func Contain(query, title string) bool {
	words := strings.Split(title, " ")
	for _, word := range words {
		stemmedword := porterstemmer.StemString(word)
		if strings.Contains(stemmedword, query) {
			return true
		}
	}
	return false
}

func (s *Searcher) load() {
	s.loadWords()
	s.loadFiles()
	s.loadIndexes()
	s.db.Close()
}

func (s *Searcher) loadWords() {
	s.words = make(map[string]int)
	rows, err := s.db.Query("select * from words")
	if err != nil {
		log.Print(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ID int
		var Name string
		err := rows.Scan(&ID, &Name)
		if err != nil {
			log.Print(err)
		}
		s.words[Name] = ID
	}
	err = rows.Err()
	if err != nil {
		log.Print(err)
	}
}

func (s *Searcher) loadFiles() {
	s.files = make(map[int]doc)

	rows, err := s.db.Query("select * from documents")

	if err != nil {
		log.Print(err)
	}
	defer rows.Close()

	var ID int
	var Title, Url, Summary string

	for rows.Next() {
		err := rows.Scan(&ID, &Title, &Url, &Summary)
		if err != nil {
			log.Print(err)
		} else {
			s.files[ID] = doc{ID, Title, Url, Summary}
		}
	}

	err = rows.Err()

	if err != nil {
		log.Print(err)
	}
}

func (s *Searcher) loadIndexes() {
	s.indexes = make(map[int][]pair)

	rows, err := s.db.Query("select * from words_documents")

	if err != nil {
		log.Print(err)
	}

	defer rows.Close()

	for rows.Next() {
		var Word_ID, Document_ID, Freq int
		err := rows.Scan(&Word_ID, &Document_ID, &Freq)
		if err != nil {
			log.Print(err)
		}
		s.indexes[Word_ID] = append(s.indexes[Word_ID], pair{Freq, Document_ID})
	}
	err = rows.Err()
	if err != nil {
		log.Print(err)
	}
}
