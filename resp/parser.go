package resp

import (
	"bufio"
	"strconv"

	"errors"
	"fishRedis/dblog"
	"io"
)

type ParsedRes struct {
	Data RedisData
	Err  error
}

type readState struct {
	bulkLen   int64
	arrayLen  int
	multiLine bool
	arrayData *ArrayData
	inArray   bool
}

func ParseStream(reader io.Reader) chan *ParsedRes {
	ch := make(chan *ParsedRes)
	go parse(reader, ch)
	return ch
}

func parse(reader io.Reader, ch chan *ParsedRes) {
	bufReader := bufio.NewReader(reader)
	state := new(readState)
	for {
		var res RedisData
		var msg []byte
		var err error
		msg, err = readLine(bufReader, state)
		if err != nil {
			if err == io.EOF {
				ch <- &ParsedRes{
					Err: io.EOF,
				}
				close(ch)
				return
			} else {
				ch <- &ParsedRes{
					Err: err,
				}
				*state = readState{}
				// can not readline just skip this round
				continue
			}
		}
		if !state.multiLine {
			// read singleLine
			// read multiline is only for bulk string
			// so when read singleLine, should handle '*' and '$' first
			// when get the '$',we read multiline next round
			if msg[0] == '*' {
				err := parseArrayHeader(msg, state)
				if err != nil {
					dblog.Logger.Errorf("parse ArrayHeader failed,message :%s", string(msg))
					ch <- &ParsedRes{
						Err: err,
					}
					*state = readState{}
				} else {
					if state.arrayLen == -1 {
						ch <- &ParsedRes{
							Data: MakeArrayData(nil),
						}
						*state = readState{}
					} else if state.arrayLen == 0 {
						ch <- &ParsedRes{
							Data: MakeArrayData([]RedisData{}),
						}
						*state = readState{}
					}
				}
				continue
			}

			if msg[0] == '$' {
				err := parseBulkHeader(msg, state)
				if err != nil {
					dblog.Logger.Errorf("parse BulkHeader failed, message : %s", string(msg))
					ch <- &ParsedRes{
						Err: err,
					}
					*state = readState{}
				} else {
					if state.bulkLen == -1 {
						// nil bulk string should not read it
						res = MakeBulkData(nil)
						state.multiLine = false
						state.bulkLen = 0
						if state.inArray {
							state.arrayData.data = append(state.arrayData.data, res)
							if len(state.arrayData.data) == state.arrayLen {
								ch <- &ParsedRes{
									Data: state.arrayData,
									Err:  nil,
								}
								*state = readState{}

							}
						} else {
							ch <- &ParsedRes{
								Data: res,
							}
						}
					}
				}
				continue
			}
			res, err = parseSingleLine(msg)
		} else {
			state.multiLine = false
			state.bulkLen = 0
			res, err = parseMultiLine(msg)
		}
		if err != nil {
			dblog.Logger.Error(err)
			ch <- &ParsedRes{
				Err: err,
			}
			*state = readState{}
			continue
		}
		if state.inArray {
			state.arrayData.data = append(state.arrayData.data, res)
			if len(state.arrayData.data) == state.arrayLen {
				ch <- &ParsedRes{
					Data: state.arrayData,
					Err:  nil,
				}
				*state = readState{}
			}
		} else {
			ch <- &ParsedRes{
				Data: res,
				Err:  err,
			}
		}
	}

}

func readLine(reader *bufio.Reader, state *readState) ([]byte, error) {
	var msg []byte
	var err error
	if state.multiLine && state.bulkLen >= 0 {
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			dblog.Logger.Error("readline bulk string failed")
			return nil, err
		}
		// after reading bulk string
		state.bulkLen = 0
		if msg[len(msg)-1] != '\n' || msg[len(msg)-2] != '\r' {
			dblog.Logger.Error("Protocol error,message is not end with CRLF")
			return nil, errors.New("Protocol error,message %s is not end with CRLF" + string(msg))
		}
	} else {
		/*normal read*/
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return msg, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			dblog.Logger.Error("Protocol error,message is not end with CRLF" + string(msg))
			return nil, errors.New("Protocol error, message is not end with CRLF" + string(msg))
		}
	}
	return msg, err

}

func parseSingleLine(msg []byte) (RedisData, error) {
	msgHeader := msg[0]
	msgBody := string(msg[1 : len(msg)-2])
	var res RedisData
	switch msgHeader {
	case '+':
		res = MakeStringData(msgBody)
	case '-':
		res = MakeErrorData(msgBody)
	case ':':
		intVal, err := strconv.ParseInt(msgBody, 10, 64)
		if err != nil {
			dblog.Logger.Error("Procotol error: msgBody can not be convert to integer")
			return nil, err
		}
		res = MakeIntData(intVal)
	default:
		// normal string, not resp
		res = MakeStringData(msgBody)
	}
	if res == nil {
		dblog.Logger.Error("Protocol error: parseSingleLine get nil data")
		return nil, errors.New("Protocol error" + string(msg))
	}
	return res, nil

}
func parseMultiLine(msg []byte) (RedisData, error) {
	if len(msg) < 2 {
		// even don't have CRLF
		return nil, errors.New("protocol error :invalid bulk string")
	}
	msgBody := msg[:len(msg)-2]
	res := MakeBulkData(msgBody)
	return res, nil
}

func parseArrayHeader(msg []byte, state *readState) error {
	arrayLen, err := strconv.Atoi(string(msg[1 : len(msg)-2]))
	if err != nil || arrayLen < -1 {
		return errors.New("protocol error,arrayLen is invalid")
	}
	state.arrayLen = arrayLen
	state.inArray = true
	state.arrayData = MakeArrayData([]RedisData{})
	return nil

}
func parseBulkHeader(msg []byte, state *readState) error {
	bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil || bulkLen < -1 {
		return errors.New("protocol error bulkLen is invalid")
	}
	state.bulkLen = bulkLen
	state.multiLine = true
	return nil
}
