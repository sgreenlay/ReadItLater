package main

import (
	"crypto/sha256"
	"context"
	"encoding/json"
	"errors"
    "flag"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

func withDatabase(op func(context.Context, *mongo.Collection) error) error {
	// Retrieve connection URI
	connectURI, foundURI := os.LookupEnv("AZURE_COSMOSDB_CONNECTION_STRING")
	if (!foundURI) {
		return errors.New("Must set AZURE_COSMOSDB_CONNECTION_STRING")
	}

	// Create a context to use with the connection
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	// Connect to the DB
	config := options.Client().ApplyURI(connectURI).SetRetryWrites(false).SetDirect(true)
	client, err := mongo.Connect(ctx, config)
	if err != nil {
		return err
	}

	// Ping the DB to confirm the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return err
	}

	// Retrieve database collection
	databaseName, foundDatabaseName := os.LookupEnv("URL_DATABASE")
	if (!foundDatabaseName) {
		databaseName = "bookmarks"
	}
	collectionName, foundCollectionName := os.LookupEnv("URL_COLLECTION")
	if (!foundCollectionName) {
		collectionName = "urls"
	}
	collection := client.Database(databaseName).Collection(collectionName)

	// Perform DB operation
	err = op(ctx, collection)
	if err != nil {
		return err
	}

	// Close the connection
	err = client.Disconnect(ctx)
	if err != nil {
		return err
	}

	return nil
}

type savedURL struct {
	ID			string  `bson:"_id"`
	URL        	string
	Description	string
	Time        string
}

func addURL(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "text/json; charset=utf-8")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	
	url := req.URL.Query().Get("url")
	if len(url) < 1 {
        return
	}
	description := req.URL.Query().Get("description")

	urlHash := fmt.Sprintf("%x", sha256.Sum256([]byte(url)))
	saveURL := savedURL{
		ID: urlHash,
		URL: url,
		Description: description,
		Time: time.Now().Format(time.RFC3339),
	}

	err := withDatabase(func(ctx context.Context, collection *mongo.Collection) error {
		_, err := collection.InsertOne(ctx, saveURL)
		if (err != nil) {
			urlFilter := bson.M{"_id": bson.M{"$eq": saveURL.ID}}
			urlFieldUpdate := bson.M{"$set": bson.M{"description": saveURL.Description}}
			_, err := collection.UpdateOne(ctx, urlFilter, urlFieldUpdate)
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func main() {
    var filePath string
	flag.StringVar(&filePath, "importFile", "", "Specify file to import into database")
	flag.Parse()
	
	if len(filePath) == 0 {
		http.HandleFunc("/api/Add", addURL)
		log.Fatal(http.ListenAndServe(":80", nil))
	} else {
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			panic(err)
		}
		
		var importURLs []savedURL
		jsonError := json.Unmarshal(data, &importURLs)
		if jsonError != nil {
			panic(jsonError)
		}

		dbError := withDatabase(func(ctx context.Context, collection *mongo.Collection) error {
			for i := range importURLs {
				importURLs[i].ID = fmt.Sprintf("%x", sha256.Sum256([]byte(importURLs[i].URL)))
				_, err := collection.InsertOne(ctx, importURLs[i])
				if err != nil {
					fmt.Println(err)
				}
			}
			return nil
		})
		if dbError != nil {
			panic(dbError)
		}
	}
}
