package server

import (
	"fmt"
	"log"
	"net"

	"github.com/AverbachDev/microservice-dados-receita/config"
	"github.com/AverbachDev/microservice-dados-receita/service"
	"github.com/robfig/cron/v3"

	"google.golang.org/grpc"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	"github.com/AverbachDev/microservice-dados-receita/db"

	"context"
)

type dadosReceitaServer struct {
	dadosReceitaPb.DadosReceitaServiceServer
}

func Start() {
	lis, err := net.Listen("tcp", ":"+config.GetYamlValues().ServerConfig.Port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	db.InitMysql()
	grpcServer := grpc.NewServer()
	dadosReceitaPb.RegisterDadosReceitaServiceServer(grpcServer, &dadosReceitaServer{})

	log.Printf("start gRPC server on %s port", config.GetYamlValues().ServerConfig.Port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

	c := cron.New()
	c.AddFunc("1 * * * *", func() { fmt.Println("Every hour on the half hour") })
	c.AddFunc("CRON_TZ=America/Sao_Paulo 30 00 1 * *", func() { service.Download() }) //download arquivos receita todo dia 1 as 00:30
	c.AddFunc("CRON_TZ=America/Sao_Paulo 30 19 1 * *", func() { stepsImport() })      //processamento da base todo dia 1 as 19:30
	c.Start()
}

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ProcessImport(ctx context.Context, req *dadosReceitaPb.WithoutParams) (*dadosReceitaPb.ServiceResponseListCnpjEmpresa, error) {

	stepsImport()

	data := &dadosReceitaPb.ListResultCnpjEmpresaData{}

	return &dadosReceitaPb.ServiceResponseListCnpjEmpresa{
		Status:  404,
		Message: "",
		Data:    data,
		Error:   "",
	}, nil
}

func stepsImport() {
	service.ProcessCSVCnae()
	service.ProcessCSVMotivoSituacaoCadastral()
	service.ProcessCSVMunicipio()
	service.ProcessCSVNaturezaJuridica()
	service.ProcessCSVPais()
	service.ProcessCSVQualificacaoSocio()
	service.ProcessCSVEmpresa()
	service.ProcessCSVSocio()
	service.ProcessCSVEstabelecimento()
	service.ProcessCSVOptanteSimples()
}
