package server

import (
	"log"
	"net"

	"github.com/AverbachDev/microservice-dados-receita/config"
	"github.com/AverbachDev/microservice-dados-receita/service"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

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

	c := cron.New()
	//c.AddFunc("* * * * *", func() { log.Printf("start gRPC server on %s port", config.GetYamlValues().ServerConfig.Port) })
	//c.AddFunc("CRON_TZ=America/Sao_Paulo 30 00 1 * *", func() { service.Download() }) //download arquivos receita todo dia 1 as 00:30
	//c.AddFunc("CRON_TZ=America/Sao_Paulo 30 19 1 * *", func() { stepsImport() })      //processamento da base todo dia 1 as 19:30
	c.AddFunc("CRON_TZ=America/Sao_Paulo 05 20 * * *", func() { service.Download() }) //download arquivos receita todo dia 1 as 00:30
	c.AddFunc("CRON_TZ=America/Sao_Paulo 30 22 * * *", func() { stepsImport() })      //processamento da base todo dia 1 as 19:30
	c.Start()

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
	)

	dadosReceitaPb.RegisterDadosReceitaServiceServer(grpcServer, &dadosReceitaServer{})

	log.Printf("start gRPC server on %s port", config.GetYamlValues().ServerConfig.Port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ProcessImport(ctx context.Context, req *dadosReceitaPb.WithoutParams) (*dadosReceitaPb.ServiceResponseProcessImport, error) {

	stepsImport()

	return &dadosReceitaPb.ServiceResponseProcessImport{
		Status:  404,
		Message: "",
		Data:    req,
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
	go service.ProcessCSVEmpresa()
	service.ProcessCSVSocio()
	service.ProcessCSVEstabelecimento()
	service.ProcessCSVOptanteSimples()
}
