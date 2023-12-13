package db

import (
	"fmt"

	"github.com/AverbachDev/microservice-dados-receita/config"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type sqlCon struct {
	DBPool *gorm.DB
}

var conn *sqlCon

type sqlConn struct {
	DbPool *gorm.DB
}

var connector *sqlConn

func InitMysql() *sqlConn {
	if connector != nil {
		log.Info("DataBase is initialized")
		return connector
	}
	log.Info("DataBase was not initialized ..initializing again")
	var err error
	connector, err = initDB()
	if err != nil {
		panic(err)
	}
	return connector
}

// DB Initialization

func initDB() (*sqlConn, error) {
	log.Info(config.GetYamlValues().DBConfig, config.GetYamlValues().DBConfig.Port)

	dbUri := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.GetYamlValues().DBConfig.Username, config.GetYamlValues().DBConfig.Password, config.GetYamlValues().DBConfig.Server, config.GetYamlValues().DBConfig.Port, config.GetYamlValues().DBConfig.Schema) //Build connection string

	db, err := gorm.Open(mysql.Open(dbUri), &gorm.Config{SkipDefaultTransaction: true})
	//db, err := gorm.Open("mysql", dbUri)
	if err != nil {
		panic(err)
	}
	/*if maxCons := config.GetYamlValues().DBConfig.MaxConnection; maxCons > 0 {
		//db.DB().SetMaxOpenConns(maxCons)
		//db.DB().SetMaxIdleConns(maxCons / 3)
	}*/
	return &sqlConn{db}, nil
}

func GetDBConnection() *gorm.DB {
	return connector.DbPool
}
