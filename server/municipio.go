package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListMunicipio(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestMunicipio) (*dadosReceitaPb.ServiceResponseListMunicipio, error) {
	result, status, err := service.ListMunicipio(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListMunicipio{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListMunicipio{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateMunicipio(ctx context.Context, req *dadosReceitaPb.MunicipioData) (*dadosReceitaPb.ServiceResponseMunicipio, error) {
	err := service.CreateMunicipio(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseMunicipio{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.MunicipioData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseMunicipio{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
