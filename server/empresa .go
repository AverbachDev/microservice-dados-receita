package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListEmpresa(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestEmpresa) (*dadosReceitaPb.ServiceResponseListEmpresa, error) {
	result, status, err := service.ListEmpresa(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListEmpresa{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListEmpresa{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateEmpresa(ctx context.Context, req *dadosReceitaPb.EmpresaData) (*dadosReceitaPb.ServiceResponseEmpresa, error) {
	err := service.CreateEmpresa(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseEmpresa{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.EmpresaData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseEmpresa{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
