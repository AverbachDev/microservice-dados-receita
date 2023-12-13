package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListSocio(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestSocio) (*dadosReceitaPb.ServiceResponseListSocio, error) {
	result, status, err := service.ListSocio(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListSocio{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListSocio{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateSocio(ctx context.Context, req *dadosReceitaPb.SocioData) (*dadosReceitaPb.ServiceResponseSocio, error) {
	err := service.CreateSocio(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseSocio{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.SocioData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseSocio{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
