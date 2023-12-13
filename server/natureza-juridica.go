package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListNaturezaJuridica(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestNaturezaJuridica) (*dadosReceitaPb.ServiceResponseListNaturezaJuridica, error) {
	result, status, err := service.ListNaturezaJuridica(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListNaturezaJuridica{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListNaturezaJuridica{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateNaturezaJuridica(ctx context.Context, req *dadosReceitaPb.NaturezaJuridicaData) (*dadosReceitaPb.ServiceResponseNaturezaJuridica, error) {
	err := service.CreateNaturezaJuridica(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseNaturezaJuridica{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.NaturezaJuridicaData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseNaturezaJuridica{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
