package mongotopsql

import (
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"mongotopsql/postgresql"
	"strings"
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
