package dblog

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var Logger *zap.SugaredLogger

func InitLogger() {

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder //指定时间格式
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoder := zapcore.NewConsoleEncoder(encoderConfig)

	//文件writeSyncer
	fileWriteSyncer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "../dblog/info.log", //日志文件存放目录
		MaxSize:    1,                   //文件大小限制,单位MB
		MaxBackups: 5,                   //最大保留日志文件数量
		MaxAge:     30,                  //日志文件保留天数
		Compress:   false,               //是否压缩处理
	})
	fileCore := zapcore.NewCore(encoder, zapcore.NewMultiWriteSyncer(fileWriteSyncer, zapcore.AddSync(os.Stdout)), zapcore.DebugLevel) //第三个及之后的参数为写入文件的日志级别,ErrorLevel模式只记录error级别的日志

	log := zap.New(fileCore, zap.AddCaller()) //AddCaller()为显示文件名和行号
	Logger = log.Sugar()
}
