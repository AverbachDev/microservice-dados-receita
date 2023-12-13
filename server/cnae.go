package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListCnae(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestCnae) (*dadosReceitaPb.ServiceResponseListCnae, error) {
	result, status, err := service.ListCnae(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListCnae{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListCnae{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateCnae(ctx context.Context, req *dadosReceitaPb.CnaeData) (*dadosReceitaPb.ServiceResponseCnae, error) {
	err := service.CreateCnae(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseCnae{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.CnaeData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseCnae{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
