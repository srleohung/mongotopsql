package mongodb

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDB struct {
	db      *mongo.Database
	Context context.Context
	Cancel  context.CancelFunc
	client  *mongo.Client
}

func NewMongoDB(url string) *MongoDB {
	ctx, cancel := context.WithCancel(context.Background())
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		panic(err)
	}
	db := client.Database("Lipios")
	return &MongoDB{db: db, Context: ctx, Cancel: cancel, client: client}
}

func (mongoDB *MongoDB) ListDatabaseNames() ([]string, error) {
	return mongoDB.client.ListDatabaseNames(context.TODO(), bson.D{{}})
}

func (mongoDB *MongoDB) ListCollectionNames() ([]string, error) {
	return mongoDB.db.ListCollectionNames(context.TODO(), bson.D{{}})
}

func (mongoDB *MongoDB) Find(collection string, projection interface{}, query interface{}) error {
	cur, err := mongoDB.db.Collection(collection).Find(mongoDB.Context, query)
	if err != nil {
		return err
	}
	defer cur.Close(mongoDB.Context)
	return cur.All(mongoDB.Context, projection)
}

func (mongoDB *MongoDB) FindAndGetCursor(collection string, query interface{}) (*mongo.Cursor, error) {
	return mongoDB.db.Collection(collection).Find(mongoDB.Context, query)
}

func (mongoDB *MongoDB) Next(cursor *mongo.Cursor) bool {
	return cursor.Next(mongoDB.Context)
}

func (mongoDB *MongoDB) All(cursor *mongo.Cursor, projection interface{}) error {
	return cursor.All(mongoDB.Context, projection)
}

func (mongoDB *MongoDB) Close(cursor *mongo.Cursor) error {
	return cursor.Close(mongoDB.Context)
}

func (mongoDB *MongoDB) Decode(cursor *mongo.Cursor, projection interface{}) error {
	return cursor.Decode(projection)
}
