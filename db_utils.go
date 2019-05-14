package database

import (
	"go.mongodb.org/mongo-driver/mongo"
)

//collection list
const(
	STOCK = "stock"
	STOCKNAME = "stockName"


)




func getColl(client *mongo.Client, collName string) *mongo.Collection{
	coll := client.Database("stock").Collection(collName)
	return coll
}