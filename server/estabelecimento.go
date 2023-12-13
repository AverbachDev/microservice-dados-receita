package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListEstabelecimento(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestEstabelecimento) (*dadosReceitaPb.ServiceResponseListEstabelecimento, error) {
	result, status, err := service.ListEstabelecimento(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListEstabelecimento{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListEstabelecimento{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateEstabelecimento(ctx context.Context, req *dadosReceitaPb.EstabelecimentoData) (*dadosReceitaPb.ServiceResponseEstabelecimento, error) {
	err := service.CreateEstabelecimento(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseEstabelecimento{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.EstabelecimentoData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseEstabelecimento{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
