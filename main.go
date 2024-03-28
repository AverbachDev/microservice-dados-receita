package main

import (
	"github.com/AverbachDev/microservice-dados-receita/config"
	"github.com/AverbachDev/microservice-dados-receita/server"
	log "github.com/sirupsen/logrus"
)

func main() {
	server.Start()
	log.Info("Listening on port:", config.GetYamlValues().ServerConfig.Port)
}
