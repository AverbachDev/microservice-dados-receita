package server

import (
	"context"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	service "github.com/AverbachDev/microservice-dados-receita/service"
)

// GetUser returns user message by user_id
func (s *dadosReceitaServer) ListQualificacaoSocio(ctx context.Context, req *dadosReceitaPb.ListCriteriaRequestQualificacaoSocio) (*dadosReceitaPb.ServiceResponseListQualificacaoSocio, error) {
	result, status, err := service.ListQualificacaoSocio(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseListQualificacaoSocio{
			Status:  status,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	return &dadosReceitaPb.ServiceResponseListQualificacaoSocio{
		Status:  status,
		Message: "",
		Data:    result,
		Error:   "",
	}, err
}

func (s *dadosReceitaServer) CreateQualificacaoSocio(ctx context.Context, req *dadosReceitaPb.QualificacaoSocioData) (*dadosReceitaPb.ServiceResponseQualificacaoSocio, error) {
	err := service.CreateQualificacaoSocio(req)

	if err != nil {
		return &dadosReceitaPb.ServiceResponseQualificacaoSocio{
			Status:  400,
			Message: err.Error(),
			Data:    nil,
			Error:   err.Error(),
		}, err
	}

	data := make([]*dadosReceitaPb.QualificacaoSocioData, 1)
	data = append(data, req)

	return &dadosReceitaPb.ServiceResponseQualificacaoSocio{
		Status:  201,
		Message: "",
		Data:    data,
		Error:   "",
	}, err
}
