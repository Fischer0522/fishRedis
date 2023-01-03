package config

import (
	"flag"
	"fmt"
	"net"
)

var Configures *Config

var (
	defaultHost     = "127.0.0.1"
	defaultPort     = 6379
	defaultLogDir   = "./"
	defaultLogLevel = "info"
	defaultShardNum = 1024
)

type Config struct {
	ConfFile string
	Host     string
	Port     int
	LogDir   string
	LogLevel string
	ShardNum int
}

type CfgError struct {
	message string
}

func (cErr *CfgError) Error() string {
	return cErr.message
}
func flagInit(cfg *Config) {
	flag.StringVar(&(cfg.ConfFile), "config", "", "Appoint a config file: such as /etc/redis.conf")
	flag.StringVar(&(cfg.Host), "host", defaultHost, "Bind host ip: default is 127.0.0.1")
	flag.IntVar(&(cfg.Port), "port", defaultPort, "Bind a listening port: default is 6379")
	flag.StringVar(&(cfg.LogDir), "logdir", defaultLogDir, "Set log directory: default is /tmp")
	flag.StringVar(&(cfg.LogLevel), "loglevel", defaultLogLevel, "Set log level: default is info")
}

func Setup() (*Config, error) {
	cfg := &Config{
		Host:     defaultHost,
		Port:     defaultPort,
		LogDir:   defaultLogDir,
		LogLevel: defaultLogLevel,
		ShardNum: defaultShardNum,
	}
	flagInit(cfg)
	flag.Parse()

	if cfg.ConfFile != "" {
		//	 parse info from file
	} else {
		if ip := net.ParseIP(cfg.Host); ip == nil {
			ipErr := &CfgError{
				message: fmt.Sprintf("Given ip address %s is invalid", cfg.Host),
			}
			return nil, ipErr
		}
		if cfg.Port <= 1024 || cfg.Port >= 65535 {
			portErr := &CfgError{
				message: fmt.Sprintf("Listen port should between 1024 and 65535,but %s is given", cfg.Port),
			}
			return nil, portErr
		}
	}
	Configures = cfg
	return cfg, nil
}
