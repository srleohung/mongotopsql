package mongotopsql

import (
	"encoding/json"
	"fmt"
	"github.com/srleohung/mongotopsql/mongodb"
	"github.com/srleohung/mongotopsql/postgresql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strings"
	"time"
)

var DELETE_APOSTROPHE bool = true
var DELETE_DOUBLE_APOSTROPHE bool = false

var SQL_DATA_TYPE map[string]string = map[string]string{
	"string":             "TEXT",
	"primitive.ObjectID": "TEXT",
	"primitive.DateTime": "TIMESTAMP WITH TIME ZONE",
	"int32":              "INT",
	"primitive.A":        "JSON",
	"primitive.M":        "JSON",
	"bool":               "Boolean",
	"float64":            "NUMERIC(15, 2)",
	"default":            "TEXT",
}

var SQL_DATA_DEFAULT map[string]string = map[string]string{
	"string":             "",
	"primitive.ObjectID": "",
	"primitive.DateTime": "",
	"int32":              "",
	"primitive.A":        "",
	"primitive.M":        "",
	"bool":               "",
	"float64":            "",
	"default":            "",
}

type MTPSynchronizer struct {
	mongo          *mongodb.MongoDB
	psql           *postgresql.PostgreSQL
	collectionName string
	monitorField   string
	lastUpdateTime time.Time
	interval       int
	stopChannel    chan bool
}

func NewMTPSynchronizer(mongo *mongodb.MongoDB, psql *postgresql.PostgreSQL, collectionName string, monitorField string, interval int) *MTPSynchronizer {
	return &MTPSynchronizer{mongo: mongo, psql: psql, collectionName: collectionName, monitorField: monitorField, lastUpdateTime: time.Now(), interval: interval, stopChannel: make(chan bool, 1)}
}

func (mtps *MTPSynchronizer) Start() error {
	t, err := mtps.psql.GetLastUpdateTime(mtps.collectionName, mtps.monitorField)
	if err != nil {
		return err
	}
	mtps.lastUpdateTime = t
	go func() {
		for {
			cursor, _ := mtps.mongo.FindAndGetCursor(mtps.collectionName, bson.M{mtps.monitorField: bson.M{"$gt": mtps.lastUpdateTime}})
			var projection bson.M
			for mtps.mongo.Next(cursor) {
				if err = mtps.mongo.Decode(cursor, &projection); err != nil {
					break
				}
				rows := GetRows(projection)
				if err := mtps.psql.InsertAndUpdate(mtps.collectionName, rows); err != nil {
					fields := GetFields(projection)
					mtps.psql.AddColumnIfNotExists(mtps.collectionName, fields)
					if err := mtps.psql.InsertAndUpdate(mtps.collectionName, rows); err != nil {
						continue
					}
				}
			}
			select {
			case <-mtps.stopChannel:
				return
			case <-time.After(time.Duration(mtps.interval) * time.Second):
				t, _ := mtps.psql.GetLastUpdateTime(mtps.collectionName, mtps.monitorField)
				mtps.lastUpdateTime = t
			}
		}
	}()
	return nil
}

func (mtps *MTPSynchronizer) Stop() {
	if len(mtps.stopChannel) > 0 {
		<-mtps.stopChannel
	}
	mtps.stopChannel <- true
}

func GetFields(projection bson.M) []postgresql.Field {
	var fields []postgresql.Field
	for k, v := range projection {
		var field postgresql.Field
		_type := fmt.Sprintf("%T", v)
		/* postgresql.Field.Name */
		field.Name = k
		/* postgresql.Field.Type */
		if t, ok := SQL_DATA_TYPE[_type]; ok {
			field.Type = t
		} else {
			field.Type = SQL_DATA_TYPE["default"]
		}
		/* postgresql.Field.Default */
		if d, ok := SQL_DATA_DEFAULT[_type]; ok {
			field.Default = d
		} else {
			field.Default = SQL_DATA_DEFAULT["default"]
		}
		fields = append(fields, field)
	}
	return fields
}

func GetRows(projection bson.M) []postgresql.Row {
	var rows []postgresql.Row
	for k, v := range projection {
		var row postgresql.Row
		_type := fmt.Sprintf("%T", v)
		/* postgresql.Row.Field */
		row.Field = k
		/* postgresql.Row.Value */
		switch _type {
		case "primitive.DateTime":
			t := fmt.Sprintf("%v", v.(primitive.DateTime).Time())
			row.Value = fmt.Sprintf("%s", t[:len(t)-3])
		case "primitive.M":
			j, _ := json.Marshal(v)
			row.Value = fmt.Sprintf("%s", j)
		case "primitive.A":
			j, _ := json.Marshal(v)
			row.Value = fmt.Sprintf("%s", j)
		default:
			row.Value = fmt.Sprintf("%v", v)
		}
		if DELETE_APOSTROPHE {
			row.Value = strings.Replace(row.Value, string(0x27), " ", -1)
		} else if DELETE_DOUBLE_APOSTROPHE {
			row.Value = strings.Replace(row.Value, string(0x22), " ", -1)
		}
		rows = append(rows, row)
	}
	return rows
}
