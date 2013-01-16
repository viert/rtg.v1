/**
 * Created with IntelliJ IDEA.
 * User: viert
 * Date: 11/29/12
 * Time: 12:29 PM
 * To change this template use File | Settings | File Templates.
 */
package web

import (
	"github.com/gorilla/mux"
	"net/http"
	"os"
	"path/filepath"
	"fmt"
	"datastore2"
	"strconv"
	"time"
	"html/template"
	"encoding/json"
)

var STATIC_DIR string
var TEMPLATE_DIR string
var ds *datastore2.DataStore

type GraphVars struct {
	Hostname		string
	Graphname		string
	Fields			[]string
	DataLength		int64
	PrefetchedData	map[string][]datastore2.DataPair
}

func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	_, err := os.Stat(STATIC_DIR + path)

	if err != nil {
	    w.WriteHeader(http.StatusNotFound)
    	fmt.Fprintln(w, "404 Not Found")
	} else {
		http.ServeFile(w, r, STATIC_DIR + path)
	}
}

func updateGraph(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	hostname := mux.Vars(r)["hostname"]
	graphname := mux.Vars(r)["graphname"]
	values := r.Form
	timestamp := time.Now().Unix()

	// special keys
	for k, v := range values {
		if k[0] != '_' { continue }
		switch k {
		case "_ts":
			value, err := strconv.ParseInt(v[0], 10, 64)
			if err == nil {
				timestamp = value
			}
		case "_expire":
			value, err := strconv.ParseInt(v[0], 10, 64)
			if err == nil {
				ds.SetExpire(hostname, graphname, value)
			}
		}
		delete(values, k)
	}


	for k, v := range values {
		value,err := strconv.ParseFloat(v[0],64)
		if err == nil {
			ds.PushData(hostname, graphname, k, value, timestamp)
		}
	}
	fmt.Fprintln(w, "ok")
}

func renderGraph(w http.ResponseWriter, r *http.Request) {

	hostname := mux.Vars(r)["hostname"]
	graphname := mux.Vars(r)["graphname"]

	fields := ds.GetFields(hostname, graphname)

	if len(fields) == 0 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "404 not found")
		return
	}


	datalen := ds.GetGraphDataMaxLength(hostname, graphname)
	li := ds.GetLatestItems(hostname, graphname, 0)

	templ, err := template.ParseFiles(TEMPLATE_DIR + "/graph.html")
	if err != nil {
		fmt.Fprintln(w, "Error executing template", err)
	} else {
		w.Header().Set("Content-Type", "text/html; charset=utf8")
		templ.Execute(w, &GraphVars{hostname, graphname, fields, datalen, li})
	}
}

func graphData(w http.ResponseWriter, r *http.Request) {

	hostname := mux.Vars(r)["hostname"]
	graphname := mux.Vars(r)["graphname"]
	timestamp, err := strconv.ParseInt(mux.Vars(r)["timestamp"], 10, 64)
	if err != nil { timestamp = 0 }

	fields := ds.GetFields(hostname, graphname)

	if len(fields) == 0 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "404 not found")
		return
	}

	li := ds.GetLatestItems(hostname, graphname, timestamp)
	data, err := json.Marshal(li)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, "500 internal server error")
		return
	}
	w.Header().Set("Content-Type","application/json")
	fmt.Fprintln(w, string(data))


}

func dumpData(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, ds.String())
}

func initRoutes() {
	r := mux.NewRouter()
	r.NotFoundHandler = http.HandlerFunc(notFoundHandler)
	r.HandleFunc("/api/update/{hostname:[0-9a-z\\.]+}/{graphname:[0-9a-z\\.]+}/", updateGraph)
	r.HandleFunc("/graph/{hostname:[0-9a-z\\.]+}/{graphname:[0-9a-z\\.]+}/", renderGraph)
	r.HandleFunc("/api/data/{hostname:[0-9a-z\\.]+}/{graphname:[0-9a-z\\.]+}/{timestamp:[0-9]+}/data.json", graphData)
	r.HandleFunc("/api/dump", dumpData)
	http.Handle("/", r)
}


func Start() {
	str, err := filepath.Abs(os.Args[0])
	if err != nil {	panic(err) }
	str = filepath.Dir(str)

	STATIC_DIR = str + "/public"
	TEMPLATE_DIR = str + "/templates"
	fmt.Println("Static directory:", STATIC_DIR)
	fmt.Println("Templates directory:", TEMPLATE_DIR)

	ds = datastore2.NewDataStore()
	initRoutes()
	http.ListenAndServe(":8000", nil)
}
