package util

import (
	"github.com/QWERKael/utility-go/log"
	"go.uber.org/zap"
)

var SugarLogger *zap.SugaredLogger

func init() {
	SugarLogger = log.InitLogger(LogPath, LogLevel)
}
