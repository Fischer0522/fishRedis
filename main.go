package main

import (
	"fishRedis/config"
	"fishRedis/dblog"
	"fishRedis/memdb"
	"fishRedis/server"
	"fmt"
	"os"
)

func init() {
	memdb.RegisterStringCommand()
	memdb.RegisterListCommands()
	memdb.RegisterHashCommand()
	memdb.RegisterSetCommands()
	memdb.RegisterKeyCommand()
	memdb.InitTransactionTable()
	memdb.RegisterTransactionCommand()
}
func main() {
	dblog.InitLogger()
	cfg, err := config.Setup()
	if err != nil {
		dblog.Logger.Fatal(err.Error())
	}
	err = server.Start(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
