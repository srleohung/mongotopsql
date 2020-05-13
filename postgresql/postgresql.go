package postgresql

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"sync"
)

type PostgreSQL struct {
	db    *sqlx.DB
	mutex *sync.RWMutex
}

type Field struct {
	Name    string
	Type    string
	Default string
}

type Row struct {
	Field string
	Value string
}

func NewPostgreSQL(url string) *PostgreSQL {
	db, err := sqlx.Connect("postgres", url)
	if err != nil {
		panic(err)
	}
	return &PostgreSQL{db: db, mutex: &sync.RWMutex{}}
}

func (postgreSQL *PostgreSQL) CreateTableIfNotExists(table string, fields []Field) {
	postgreSQL.mutex.RLock()
	defer postgreSQL.mutex.RUnlock()
	schema := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", table)
	for i, field := range fields {
		if i == len(fields)-1 {
			if field.Default == "" {
				schema = fmt.Sprintf("%s \"%s\" %s, UNIQUE(_id));", schema, field.Name, field.Type)
			} else {
				schema = fmt.Sprintf("%s \"%s\" %s DEFAULT '%s', UNIQUE(_id));", schema, field.Name, field.Type, field.Default)
			}
		} else {
			if field.Default == "" {
				schema = fmt.Sprintf("%s \"%s\" %s,", schema, field.Name, field.Type)
			} else {
				schema = fmt.Sprintf("%s \"%s\" %s DEFAULT '%s',", schema, field.Name, field.Type, field.Default)
			}
		}
	}
	postgreSQL.db.MustExec(schema)
}

func (postgreSQL *PostgreSQL) Insert(table string, rows []Row) error {
	postgreSQL.mutex.RLock()
	defer postgreSQL.mutex.RUnlock()
	schema := fmt.Sprintf("INSERT INTO %s (", table)
	var value string
	for i, row := range rows {
		if i == len(rows)-1 {
			value = fmt.Sprintf("%s '%s') ON CONFLICT (\"_id\") DO NOTHING;", value, row.Value)
			schema = fmt.Sprintf("%s %s) VALUES (%s", schema, row.Field, value)
		} else {
			value = fmt.Sprintf("%s '%s',", value, row.Value)
			schema = fmt.Sprintf("%s %s,", schema, row.Field)
		}
	}
	_, err := postgreSQL.db.Exec(schema)
	return err
}

func (postgreSQL *PostgreSQL) AddColumnIfNotExists(table string, fields []Field) {
	postgreSQL.mutex.RLock()
	defer postgreSQL.mutex.RUnlock()
	var schema string
	for _, field := range fields {
		if field.Default == "" {
			schema = fmt.Sprintf("ALTER TABLE %s ADD %s %s;", table, field.Name, field.Type)
		} else {
			schema = fmt.Sprintf("ALTER TABLE %s ADD %s %s DEFAULT '%s';", table, field.Name, field.Type, field.Default)
		}
		postgreSQL.db.Exec(schema)
	}
}
