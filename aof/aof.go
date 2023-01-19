package aof

import (
	"bufio"
	"fishRedis/dblog"
	"fishRedis/resp"
	"os"
)

var fileName = "aof.data"

func AofWrite(buffer chan []byte) {

	for command := range buffer {
		aofFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			dblog.Logger.Error("An error occurred when opening a file", err)
			aofFile.Close()

		}
		writer := bufio.NewWriter(aofFile)
		_, err = writer.Write(command)
		if err != nil {
			dblog.Logger.Error("write aof failed")
		}
		//fmt.Println("writing:", string(command))
		writer.Flush()
		aofFile.Close()
	}
}
func LoadAofFromDisk() chan *resp.ParsedRes {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		dblog.Logger.Info("create a new aof file")
	}
	aofFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		dblog.Logger.Error("cannot open aof file")
		return nil
	}
	return resp.ParseStream(aofFile)
}
