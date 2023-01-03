package server

import (
	"fishRedis/dblog"
	"fishRedis/memdb"
	"fishRedis/resp"
	"io"
	"net"
)

type Handler struct {
	memdb *memdb.MemDb
}

func NewHandler() *Handler {
	return &Handler{
		memdb: memdb.NewMemdb(),
	}
}

func (h *Handler) handle(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			dblog.Logger.Error(err)
		}
	}()
	ch := resp.ParseStream(conn)
	for parsedRes := range ch {
		if parsedRes.Err != nil {
			if parsedRes.Err == io.EOF {
				dblog.Logger.Info("Close connection ", conn.RemoteAddr().String())
			} else {
				dblog.Logger.Panic("handle connection ", conn.RemoteAddr().String())
			}
			return
		}
		if parsedRes.Data == nil {
			dblog.Logger.Error("empty parsedRes.Data from ", conn.RemoteAddr().String())
		}
		arrayData, ok := parsedRes.Data.(*resp.ArrayData)
		if !ok {
			dblog.Logger.Error("parsedRes.Data is not ArrayData from ", conn.RemoteAddr().String())
		}
		cmd := arrayData.ToCommand()
		res := h.memdb.ExecCommand(cmd)
		if res != nil {
			_, err := conn.Write(res.ToBytes())
			if err != nil {
				dblog.Logger.Error("write response to ", conn.RemoteAddr().String())
			}
		} else {
			errData := resp.MakeErrorData("unknown error")
			_, err := conn.Write(errData.ToBytes())
			if err != nil {
				dblog.Logger.Error("write response to ", conn.RemoteAddr().String())
			}
		}

	}
}
