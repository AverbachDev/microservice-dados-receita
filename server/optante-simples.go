package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListOptanteSimples(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestOptanteSimples) (*dadosReceitaPb.ServiceResponseListOptanteSimples, error) {
	result, status, err := service.ListOptanteSimples(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListOptanteSimples{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListOptanteSimples{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateOptanteSimples(ctx context.Context, req *dadosReceitaPb.OptanteSimplesData) (*dadosReceitaPb.ServiceResponseOptanteSimples, error) {
	err := service.CreateOptanteSimples(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseOptanteSimples{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.OptanteSimplesData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseOptanteSimples{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
