package go_migrate

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	MigrationsPath          = "./migrates/"
	UpMigrationsPath        = "./migrates/up/"
	DownMigrationsPath      = "./migrates/down/"
	DatabaseVersionFilePath = "./migrates/version"
	SchemaFilePath          = "./migrates/schema.sql"
)

func Install() {
	os.Mkdir(MigrationsPath, os.ModePerm)
	os.Mkdir(UpMigrationsPath, os.ModePerm)
	os.Mkdir(DownMigrationsPath, os.ModePerm)
	os.Create(DatabaseVersionFilePath)
	os.Create(SchemaFilePath)
}

func NewMigrate(name string) {
	prefix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)

	upName := UpMigrationsPath + prefix + "_" + name + ".sql"
	os.Create(upName)
	downName := DownMigrationsPath + prefix + "_" + name + ".sql"
	os.Create(downName)

	fmt.Println("generate up sql file:", upName)
	fmt.Println("generate down sql file:", downName)
}

func Migrate(db *sql.DB) {
	curVersion := curVersion()
	filePathes, _ := filepath.Glob(UpMigrationsPath + "*.sql")
	sort.Sort(sort.StringSlice(filePathes))

	for _, filePath := range filePathes {
		if curVersion < path.Base(filePath) {
			execWithFile(db, filePath)
			fmt.Println("Migrate", filePath)

			curVersion = path.Base(filePath)
			ioutil.WriteFile(DatabaseVersionFilePath, []byte(curVersion), os.ModePerm)
		}
	}
	RefreshSchema(db)
}

func Rollback(db *sql.DB) {
	filePathes, _ := filepath.Glob(DownMigrationsPath + "*.sql")
	curVersion := curVersion()
	preVersion := preVersion(filePathes, curVersion)
	fmt.Println("Current Version is", curVersion)
	if curVersion > preVersion {
		execWithFile(db, DownMigrationsPath+curVersion)
		ioutil.WriteFile(DatabaseVersionFilePath, []byte(preVersion), os.ModePerm)
	}
	fmt.Println("Rollback to", preVersion)
	RefreshSchema(db)
}

func RefreshSchema(db *sql.DB) {
	file, err := os.Create(SchemaFilePath)
	defer file.Close()
	if err != nil {
		panic(err)
	}

	var tableName string
	rows, err := db.Query("SHOW TABLES;")
	if err != nil {
		panic(err)
	}

	var schemaContent string
	for rows.Next() {
		rows.Scan(&tableName)
		queryString := "SHOW CREATE TABLE " + tableName + ";"

		var tableDescribe string
		err = db.QueryRow(queryString).Scan(&tableName, &tableDescribe)
		if err != nil {
			panic(err)
		}
		schemaContent += tableDescribe + ";\n\n\n"
	}
	file.WriteString(strings.TrimSpace(schemaContent))
}

func preVersion(filePathes []string, curVersion string) string {
	sort.Sort(sort.Reverse(sort.StringSlice(filePathes)))
	if len(filePathes) < 2 {
		return ""
	}
	for _, filePath := range filePathes {
		filePathVersion := path.Base(filePath)
		if filePathVersion < curVersion {
			return filePathVersion
		}
	}
	return "0"
}

func execWithFile(db *sql.DB, filePath string) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	contentStr := string(content)
	sqls := strings.Split(contentStr, ";")
	tx, _ := db.Begin()
	for _, sql := range sqls {
		sql = strings.TrimSpace(sql)
		if len(sql) == 0 {
			continue
		}
		_, err = tx.Exec(sql)
		if err != nil {
			tx.Rollback()
			fmt.Println("error sql is", sql)
			panic(err)
		}
	}
	tx.Commit()
}

func curVersion() string {
	curVersion, err := ioutil.ReadFile(DatabaseVersionFilePath)
	if err != nil {
		os.Create(DatabaseVersionFilePath)
	}
	return strings.Trim(string(curVersion), " \n")
}
