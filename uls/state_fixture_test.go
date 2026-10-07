package uls

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sourceRow(kind string, id int, call, status, state string) string {
	fields := make([]string, 59)
	fields[0], fields[1], fields[4] = kind, fmt.Sprint(id), call
	if kind == "HD" {
		fields[5] = status
	}
	if kind == "EN" {
		fields[5], fields[15], fields[16], fields[17], fields[18] = status, "DISTRACTOR ADDRESS", "DISTRACTOR CITY", state, "12345"
	}
	return strings.Join(fields, "|") + "\n"
}

func stateSources() map[string]string {
	files := map[string]string{}
	for id, call := range []string{"K1ABC", "K2ABC", "K3ABC", "K4ABC", "K5ABC", "K6ABC", "K7ABC"} {
		files["HD.DAT"] += sourceRow("HD", id+1, call, "A", "")
		files["AM.DAT"] += sourceRow("AM", id+1, call, "", "")
	}
	files["HD.DAT"] += sourceRow("HD", 8, "K8ABC", "E", "")
	files["AM.DAT"] += sourceRow("AM", 8, "K8ABC", "", "")
	files["EN.DAT"] = sourceRow("EN", 1, "k1abc", "L", " ca ") + sourceRow("EN", 1, "K1ABC", "L", "CA") +
		sourceRow("EN", 1, "K1ABC", "CL", "NY") + sourceRow("EN", 2, "K2ABC", "L", "") +
		sourceRow("EN", 3, "K3ABC", "L", "TX") + sourceRow("EN", 3, "K3ABC", "L", "FL") +
		sourceRow("EN", 4, "K4ABC", "L", "NOT-A-STATE") + sourceRow("EN", 4, "K4ABC", "L", "WA") +
		sourceRow("EN", 5, "K5ABC", "L", "") + sourceRow("EN", 5, "K5ABC", "L", "WA") +
		sourceRow("EN", 6, "K6ABC", "L", "NY") + sourceRow("EN", 6, "WRONGCALL", "L", "NY") +
		sourceRow("EN", 8, "K8ABC", "L", "CA")
	return files
}

func writeSources(t testing.TB, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func fixtureDB(t testing.TB, legacy bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fcc.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	ddl := "CREATE TABLE AM(call_sign TEXT); INSERT INTO AM VALUES('K1ABC');"
	if !legacy {
		ddl = "CREATE TABLE AM(call_sign TEXT,state TEXT); CREATE INDEX idx_AM_call_sign ON AM(call_sign); INSERT INTO AM VALUES('K1ABC','CA'); INSERT INTO AM VALUES('K2ABC',''); PRAGMA user_version=1;"
	}
	if _, err := db.ExecContext(context.Background(), ddl); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func zippedSources(t testing.TB, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	z := zip.NewWriter(&buf)
	for name, data := range files {
		w, err := z.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
