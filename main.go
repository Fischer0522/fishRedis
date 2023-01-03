package main

import (
	"fishRedis/config"
	"fishRedis/dblog"
	"fishRedis/server"
	"os"
)

func main() {
	dblog.InitLogger()
	cfg, err := config.Setup()
	if err != nil {
		dblog.Logger.Fatal(err.Error())
	}
	err = server.Start(cfg)
	if err != nil {
		os.Exit(1)
	}

}
