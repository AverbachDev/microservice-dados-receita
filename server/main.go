package server

import (
	"fmt"
	"log"
	"net"

	"github.com/AverbachDev/microservice-dados-receita/config"
	"github.com/AverbachDev/microservice-dados-receita/service"
	"github.com/robfig/cron/v3"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

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
	c.AddFunc("CRON_TZ=America/Sao_Paulo 30 00 1 * *", func() { service.Download() }) //download arquivos receita todo dia 1 as 00:30
	c.AddFunc("CRON_TZ=America/Sao_Paulo 30 19 1 * *", func() { stepsImport() })      //processamento da base todo dia 1 as 19:30
	//c.AddFunc("CRON_TZ=America/Sao_Paulo 00 17 * * *", func() { service.Download() }) //download arquivos receita todo dia 1 as 00:30
	//c.AddFunc("CRON_TZ=America/Sao_Paulo 30 21 * * *", func() { stepsImport() })      //processamento da base todo dia 1 as 19:30
	c.Start()

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(serverInterceptor),
	)

	dadosReceitaPb.RegisterDadosReceitaServiceServer(grpcServer, &dadosReceitaServer{})

	log.Printf("start gRPC server on %s port", config.GetYamlValues().ServerConfig.Port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}

func serverInterceptor(ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("couldn't parse client IP address")
	}

	host, port, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return "", fmt.Errorf("couldn't parse client IP address")
	}

	log.Println("host:", host, "port:", port, "err:", err)
	if len(host) > 5 {
		trustedIPs := config.GetYamlValues().ServerConfig.TrustedIPs

		for _, n := range trustedIPs {
			if host == n {
				h, err := handler(ctx, req)

				return h, err
			}
		}
		// Calls the handler
		return "", fmt.Errorf("couldn't parse client IP address")
	}

	// Calls the handler
	h, err := handler(ctx, req)

	return h, err
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
	service.ProcessCSVEmpresa()
	service.ProcessCSVSocio()
	service.ProcessCSVEstabelecimento()
	service.ProcessCSVOptanteSimples()
}
