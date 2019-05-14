package database

import (
	"aliyun.com/papaya/utils"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
	"context"
)

const(
	MAX_CONNECTION = 10
	INITIAL_CONNECTION = 4
	AVAILABLE = false
	USED = true

)

var mu sync.RWMutex

/*
clientList: the client pool
clientAvailable: the available flag, means the location and available flag in the  client pool
size: the size of allocated client pool <= MAX_CONNECTION
*/
type mongodata struct{
	client *mongo.Client
	pos int
	flag bool

}

type ClientPool struct{
	clientList [MAX_CONNECTION]mongodata
	size int
}

var cp ClientPool

//initial the connection to the pool
func init(){
	for size := 0;  size < INITIAL_CONNECTION || size < MAX_CONNECTION; size++ {
		err := cp.allocateCToPool(size)
		utils.Logger.SetPrefix("WARNING ")
		utils.Logger.Println("init - initial create the connect pool failed, size:", size,  err)
	}
}

func Dbconnect() (client *mongo.Client, err error){
	client, err = mongo.NewClient(options.Client().ApplyURI("mongodb://192.168.0.2:8998"))
	if err != nil {
		utils.Logger.SetPrefix("WARNING ")
		utils.Logger.Println("Dbconnect - connect mongodb failed", err)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		utils.Logger.SetPrefix("WARNING ")
		utils.Logger.Println("Dbconnect - connect mongodb ctx failed", err)
		return nil, err
	}

	return client,nil
}

func Dbdisconnect(client *mongo.Client) (err error){
	err = client.Disconnect(context.TODO())
	if err != nil {
		utils.Logger.SetPrefix("WARNING ")
		utils.Logger.Println("Dbdisconnect - disconnect mongodb failed", err)
	}
	return err
}

//create a new database connection to the pool
func (cp *ClientPool) allocateCToPool(pos int) (err error){
	cp.clientList[pos].client, err = Dbconnect()
	if err != nil {
		utils.Logger.SetPrefix("WARNING ")
		utils.Logger.Println("allocateCToPool - allocateCToPool failed,position: ", pos, err)
		return err
	}

	cp.clientList[pos].flag = USED
	cp.clientList[pos].pos = pos
	return nil
}

//apply a connection from the pool
func (cp *ClientPool) getCToPool(pos int){
	cp.clientList[pos].flag = USED
}

//free a connection back to the pool
func (cp *ClientPool) putCBackPool(pos int){
	cp.clientList[pos].flag = AVAILABLE
}

//program apply a database connection
func GetClient() (mongoclient *mongodata,  err error) {
	mu.RLock()
	for i:=1; i<cp.size; i++ {
		if cp.clientList[i].flag == AVAILABLE{
			return &cp.clientList[i], nil
		}
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()
	if cp.size < MAX_CONNECTION{
		err = cp.allocateCToPool(cp.size)
		if err != nil {
			utils.Logger.SetPrefix("WARNING ")
			utils.Logger.Println("GetClient - DB pooling allocate failed", err)
			return nil, err
		}

		pos := cp.size
		cp.size++
		return &cp.clientList[pos], nil
	} else {
		utils.Logger.SetPrefix("WARNING ")
		utils.Logger.Println("GetClient - DB pooling is fulled")
		return nil, errors.New("DB pooling is fulled")
	}

}

//program release a connection
func ReleaseClient(mongoclient *mongodata){
	mu.Lock()
	cp.putCBackPool(mongoclient.pos)
	mu.Unlock()
}