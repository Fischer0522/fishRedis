package server

import (
	"fishRedis/aof"
	"fishRedis/config"
	"fishRedis/dblog"
	"fishRedis/memdb"
	"fishRedis/resp"
	"io"
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
	//dblog.Logger.Info("server listen at", cfg.Host, ":", cfg.Port)
	//channels := initGoroutines(16, 100)
	//handler := NewHandler()
	//handleConn(channels,handler)
	//for {
	//	conn, err := listener.Accept()
	//	if err != nil {
	//		dblog.Logger.Error(err)
	//		break
	//	}
	//	dblog.Logger.Info(conn.RemoteAddr().String(), "connected")
	//	rand.Seed(time.Now().Unix())
	//	index := rand.Int() % 16
	//	channels[index] <- conn
	//
	//}
	//return nil

	var sg sync.WaitGroup
	handler := NewHandler()
	initFromAof(handler)
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
			handler.readQueryFromClient(conn, cfg.AofIsOn)
		}()
	}
	sg.Wait()
	return nil
}
func initFromAof(handler *Handler) {
	fakeClient := memdb.NewRedisClient()
	fakeClient.RedisDb = handler.memdb
	ch := aof.LoadAofFromDisk()
	if ch == nil {
		return
	}
	for parsedRes := range ch {
		if parsedRes.Err != nil {
			if parsedRes.Err == io.EOF {
				return
			} else {
			}
		}
		if parsedRes.Data == nil {

		}
		arrayData, ok := parsedRes.Data.(*resp.ArrayData)
		if !ok {
			dblog.Logger.Error("parsedRes.Data is not ArrayData  ")
			continue
		}
		cmd := arrayData.ToCommand()
		fakeClient.Args = cmd
		ProcessCommand(fakeClient)
	}

}

//func initGoroutines(size int, bufferSize int) []chan net.Conn {
//	channels := make([]chan net.Conn, size)
//	for i := 0; i < size; i++ {
//		channels[i] = make(chan net.Conn, bufferSize)
//	}
//	return channels
//}
//func handleConn(channels []chan net.Conn, handler *Handler) {
//	for i := 0; i < len(channels); i++ {
//		go func(index int) {
//			for element := range channels[index] {
//				handler.readQueryFromClient(element)
//			}
//		}(i)
//	}
//}
