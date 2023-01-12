package server

import (
	"fishRedis/config"
	"fishRedis/dblog"
	"net"
	"strconv"
	"sync"
)

func Start(cfg *config.Config) error {
	listener, err := net.Listen("tcp", cfg.Host+":"+strconv.Itoa(cfg.Port))
	if err != nil {
		dblog.Logger.Panic(err)
		return err
	}
	defer func() {
		err := listener.Close()
		if err != nil {
			dblog.Logger.Error(err)
		}
	}()
	dblog.Logger.Info("server listen at", cfg.Host, ":", cfg.Port)

	var sg sync.WaitGroup
	handler := NewHandler()

	for {
		conn, err := listener.Accept()
		if err != nil {
			dblog.Logger.Error(err)
			break
		}
		dblog.Logger.Info(conn.RemoteAddr().String(), "connected")
		sg.Add(1)
		go func() {
			defer sg.Done()
			handler.readQueryFromClient(conn)
		}()
	}
	sg.Wait()
	return nil
}
