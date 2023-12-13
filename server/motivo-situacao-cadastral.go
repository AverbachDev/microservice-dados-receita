package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListMotivoSituacaoCadastral(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestMotivoSituacaoCadastral) (*dadosReceitaPb.ServiceResponseListMotivoSituacaoCadastral, error) {
	result, status, err := service.ListMotivoSituacaoCadastral(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListMotivoSituacaoCadastral{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListMotivoSituacaoCadastral{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateMotivoSituacaoCadastral(ctx context.Context, req *dadosReceitaPb.MotivoSituacaoCadastralData) (*dadosReceitaPb.ServiceResponseMotivoSituacaoCadastral, error) {
	err := service.CreateMotivoSituacaoCadastral(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseMotivoSituacaoCadastral{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.MotivoSituacaoCadastralData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseMotivoSituacaoCadastral{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
