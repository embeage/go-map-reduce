package mr

import (
	"fmt"
	"log"
	"os"
	"time"
)

var (
	WarningLogger *log.Logger
	InfoLogger    *log.Logger
	ErrorLogger   *log.Logger
)

func InitLogger(caller string) {
	logname := fmt.Sprintf("%s_log_%d", caller, time.Now().UnixNano())
	f, err := os.OpenFile(logname, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil {
		log.Fatal(err)
	}

	InfoLogger = log.New(f, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WarningLogger = log.New(f, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger = log.New(f, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}
