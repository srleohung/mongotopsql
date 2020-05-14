package main

import (
	"fmt"
	. "github.com/srleohung/mongotopsql"
	"github.com/srleohung/mongotopsql/mongodb"
	"github.com/srleohung/mongotopsql/postgresql"
	"go.mongodb.org/mongo-driver/bson"
	"log"
	"strings"
	"sync"
)

func main() {
	/* New MongoDB */
	mongoURL := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", "username", "password", "localhost", "27017", "database")
	mongo := mongodb.NewMongoDB(mongoURL)

	/* List Database Names */
	/*
		result, err := mongo.ListDatabaseNames()
		if err != nil {
			log.Print(err)
		}
		for _, r := range result {
			fmt.Println(r)
		}
	*/

	/* List Collection Names */
	result, _ := mongo.ListCollectionNames()
	/*
		if err != nil {
			log.Print(err)
		}
		for _, r := range result {
			fmt.Println(r)
		}
	*/

	/* New PostgreSQL */
	psqlURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", "username", "password", "localhost", "5432", "database", "disable")
	psql := postgresql.NewPostgreSQL(psqlURL)

	/* Find */
	var wg sync.WaitGroup
	wg.Add(len(result))
	for _, collection := range result {
		/* Skip System Collections */
		if strings.Contains(collection, "system.") {
			wg.Done()
			continue
		}
		go func(collection string) {
			defer wg.Done()
			cursor, _ := mongo.FindAndGetCursor(collection, bson.M{})
			var projection bson.M
			var isCreated bool
			for mongo.Next(cursor) {
				if err := mongo.Decode(cursor, &projection); err != nil {
					panic(err)
				}
				/* Create Table If Not Exists */
				if !isCreated {
					fields := GetFields(projection)
					psql.CreateTableIfNotExists(collection, fields)
					log.Printf("Created Table \"%v\"\n", collection)
					isCreated = !isCreated
				}
				/* Insert */
				rows := GetRows(projection)
				if err := psql.Insert(collection, rows); err != nil {
					/* Add Column If Not Exists */
					fields := GetFields(projection)
					psql.AddColumnIfNotExists(collection, fields)
					if err := psql.Insert(collection, rows); err != nil {
						/* panic(err) */
						continue
					}
				}
			}
			log.Printf("Inserted \"%v\"\n", collection)
		}(collection)
	}
	wg.Wait()
	log.Printf("Successfully switched MongoDB to PostgreSQL")

	/* New Mongo To PostgreSQL Synchronizer */
	mtps := NewMTPSynchronizer(mongo, psql, "collectionName", "monitorField", 1)
	err := mtps.Start()
	if err != nil {
		log.Print(err)
	}

	/* Stop Synchronizer */
	/*
		mtps.Stop()
	*/

	/* Forever */
	forever := make(chan bool, 1)
	<-forever
}
