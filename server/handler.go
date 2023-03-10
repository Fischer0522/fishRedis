package server

import (
	"fishRedis/aof"
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

func (h *Handler) readQueryFromClient(conn net.Conn, aofIsOn bool) {
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
	redisClient := memdb.NewRedisClient()
	aofChan := make(chan []byte, 1000)
	if aofIsOn {
		go aof.AofWrite(aofChan)
	}

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
		aofChan <- arrayData.ToBytes()
		cmd := arrayData.ToCommand()
		redisClient.Args = cmd
		redisClient.Conn = conn
		redisClient.RedisDb = h.memdb
		ProcessCommand(redisClient)
		sendReplyToClient(redisClient)

	}
}
func ProcessCommand(redisClient *memdb.RedisClient) {
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
	// if is in the transaction mode
	if redisClient.Flags&memdb.REDIS_MULTI == memdb.REDIS_MULTI {

		// if cmdName is EXEC DISCARD WATCH MULTI
		_, ok := memdb.TransactionTable[cmdName]

		if redisClient.RedisCommand != nil {
			execFunc := *redisClient.RedisCommand
			if ok {
				redisClient.OutputBuf = execFunc(redisClient)
			} else {
				redisClient.Mstate.AddCommandToBuf(cmd, &execFunc)
				redisClient.OutputBuf = resp.MakeStringData("QUEUED")
			}
		}

	} else {
		if redisClient.RedisCommand != nil {
			execFunc := *redisClient.RedisCommand
			redisClient.OutputBuf = execFunc(redisClient)
		}

	}

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
