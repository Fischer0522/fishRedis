package server

import (
	"fishRedis/dblog"
	"fishRedis/memdb"
	"fishRedis/resp"
	"io"
	"net"
	"strings"
)

type Handler struct {
	memdb *memdb.MemDb
}

func NewHandler() *Handler {
	return &Handler{
		memdb: memdb.NewMemdb(),
	}
}

func (h *Handler) readQueryFromClient(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			dblog.Logger.Error(err)
		}
		if er := recover(); er != nil {
			dblog.Logger.Error(er)
		}
	}()
	ch := resp.ParseStream(conn)
	for parsedRes := range ch {
		if parsedRes.Err != nil {
			if parsedRes.Err == io.EOF {
				dblog.Logger.Info("Close connection ", conn.RemoteAddr().String())
				continue
			} else {
				dblog.Logger.Panic("handle connection ", conn.RemoteAddr().String(), "panic:", parsedRes.Err.Error())
			}
		}
		if parsedRes.Data == nil {
			dblog.Logger.Error("empty parsedRes.Data from ", conn.RemoteAddr().String())

		}
		arrayData, ok := parsedRes.Data.(*resp.ArrayData)
		if !ok {
			dblog.Logger.Error("parsedRes.Data is not ArrayData from ", conn.RemoteAddr().String())
			continue
		}
		cmd := arrayData.ToCommand()
		redisClient := memdb.NewRedisClient()
		redisClient.Args = cmd
		redisClient.Conn = conn
		redisClient.RedisDb = h.memdb
		processCommand(redisClient)

	}
}
func processCommand(redisClient *memdb.RedisClient) {
	cmd := redisClient.Args
	if len(cmd) == 0 {
		return
	}
	cmdName := strings.ToLower(string(cmd[0]))
	cmdExecutor, ok := memdb.CmdTable[cmdName]
	if !ok {
		redisClient.OutputBuf = resp.MakeStringData("error unsupported command")
	}
	redisClient.RedisCommand = cmdExecutor
	if redisClient.RedisCommand != nil {
		execFunc := *redisClient.RedisCommand
		redisClient.OutputBuf = execFunc(redisClient)
	}

	sendReplyToClient(redisClient)
}

func sendReplyToClient(redisClient *memdb.RedisClient) {
	conn := redisClient.Conn

	var res []byte
	if redisClient.OutputBuf != nil {
		res = redisClient.OutputBuf.ToBytes()

	} else {
		res = []byte("unknown error")
	}
	_, err := conn.Write(res)
	if err != nil {
		dblog.Logger.Error("write response to ", conn.RemoteAddr().String())
	}

}
