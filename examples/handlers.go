package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"goji.io/pat"
)

func handleStatus(w http.ResponseWriter, r *http.Request) {
	status := make(map[string]string)
	status["status"] = "ok"
	b, _ := json.Marshal(status)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(b)

}

func handleGetKey(w http.ResponseWriter, r *http.Request) {
	bucket := pat.Param(r, "bucket")
	key := pat.Param(r, "key")

	v, err := db.Get(bucket, key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if v == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octect-stream")
	io.WriteString(w, v)
}

func handleSetKey(w http.ResponseWriter, r *http.Request) {
	bucket := pat.Param(r, "bucket")
	key := pat.Param(r, "key")

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	val := string(body)

	err = db.Set(bucket, key, val)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
func handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	bucket := pat.Param(r, "bucket")
	key := pat.Param(r, "key")

	err := db.Delete(bucket, key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	bucket := pat.Param(r, "bucket")

	err := db.CreateBucket(bucket)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := pat.Param(r, "bucket")

	err := db.DeleteBucket(bucket)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets := db.ListBuckets()
	res := &struct {
		Buckets []string `json:"buckets"`
	}{
		Buckets: buckets,
	}
	b, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}
