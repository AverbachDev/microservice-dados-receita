package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListPais(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestPais) (*dadosReceitaPb.ServiceResponseListPais, error) {
	result, status, err := service.ListPais(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListPais{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListPais{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreatePais(ctx context.Context, req *dadosReceitaPb.PaisData) (*dadosReceitaPb.ServiceResponsePais, error) {
	err := service.CreatePais(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponsePais{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.PaisData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponsePais{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
