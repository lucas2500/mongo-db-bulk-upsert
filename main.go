package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client   *mongo.Client
	MongoCtx context.Context
)

type Car struct {
	Name         string `bson:"Name"`
	Year         string `bson:"Year"`
	Manufacturer string `bson:"Manufacturer"`
}

func main() {

	ConnectToMongoDB()
	defer DisconnectFromMongoDB()

	UpsertData()
}

func UpsertData() {

	var models []mongo.WriteModel

	cars := []Car{
		{
			Name:         "BMW M3",
			Year:         "2021",
			Manufacturer: "BMW",
		},
		{
			Name:         "Nissan GTR",
			Year:         "2010",
			Manufacturer: "Nissan",
		},
		{
			Name:         "Corsa",
			Year:         "2005",
			Manufacturer: "Chevrolet",
		},
		{
			Name:         "Viper",
			Year:         "2012",
			Manufacturer: "Dodge",
		},
	}

	coll := client.Database("vehicles").Collection("cars")

	for i := range cars {
		models = append(models,
			mongo.NewUpdateOneModel().
				// Filter
				SetFilter(bson.D{{Key: "Name", Value: cars[i].Name}}).
				// Body
				SetUpdate(bson.D{{Key: "$set", Value: cars[i]}}).
				SetUpsert(true),
		)
	}

	_, err := coll.BulkWrite(MongoCtx, models)

	FailOnError(err, "There was an error upserting data", true)

	log.Println("- collection upserted successfully")
}

func ConnectToMongoDB() {

	var err error

	log.Println("- Starting Mongo connection...")

	MongoCtx = context.Background()
	client, err = mongo.Connect(MongoCtx, options.Client().ApplyURI("mongodb://root:root@localhost:27017/"))

	FailOnError(err, "There was an error connecting to Mongo", true)

	log.Println("Connected successfully!!")
}

func DisconnectFromMongoDB() {

	log.Println("- Starting Mongo disconnection")

	err := client.Disconnect(MongoCtx)

	FailOnError(err, "There was an error disconnecting from Mongo", true)

	log.Println("Disconnected successfully!!")

}

func FailOnError(err error, msg string, kill bool) {

	if err != nil {
		if !kill {
			log.Printf("%s: %s", msg, err)
		} else {
			log.Fatalf("%s: %s", msg, err)
		}
	}
}
