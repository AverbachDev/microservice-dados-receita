package service

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	dbService "github.com/AverbachDev/microservice-dados-receita/db"
	"github.com/AverbachDev/microservice-dados-receita/entity"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/charmap"
	"gorm.io/gorm"
)

func ListEmpresa(data *dadosReceitaPb.ListCriteriaRequestEmpresa) (*dadosReceitaPb.ListResultEmpresa, int32, error) {
	var record []entity.Empresa
	var result *dadosReceitaPb.ListResultEmpresa
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.RazaoSocial != "" {
		fields = append(fields, "razao_social LIKE ? ")
		values = append(values, "%"+data.RazaoSocial+"%")
	}

	if data.Id != "" {
		fields = append(fields, "id LIKE ? ")
		values = append(values, "%"+data.Id+"%")
	}

	if err := db.Table("empresa").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultEmpresa{
		Result: convertListEmpresaToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateEmpresa(data *dadosReceitaPb.EmpresaData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("empresa").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func ListCnpjEmpresa(data *dadosReceitaPb.ListCriteriaRequestCnpjEmpresa) (*dadosReceitaPb.ListResultCnpjEmpresaData, int32, error) {
	var record []entity.CNPJEmpresa
	var result *dadosReceitaPb.ListResultCnpjEmpresaData
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.CdCnpjCpf != "" {
		fields = append(fields, "cnpj = ? ")
		values = append(values, data.CdCnpjCpf)
	}

	if err := db.Table("view_empresa").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultCnpjEmpresaData{
		Result: convertListCnpjEmpresaToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func ListSocioEmpresa(data *dadosReceitaPb.ListCriteriaRequestSocioEmpresa) (*dadosReceitaPb.ListResultSocioEmpresaData, int32, error) {
	var record []entity.SocioEmpresa
	var result *dadosReceitaPb.ListResultSocioEmpresaData
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.IdEmpresa != "" {
		fields = append(fields, "id_empresa = ? ")
		values = append(values, data.IdEmpresa)
	}

	if err := db.Table("view_socio").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultSocioEmpresaData{
		Result: convertListSocioEmpresaToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func structDataEmpresaToRes(data entity.Empresa) *dadosReceitaPb.EmpresaData {

	d := &dadosReceitaPb.EmpresaData{
		Id:                      data.Id,
		RazaoSocial:             data.RazaoSocial,
		CodigoNaturezaJuridica:  data.CodigoNaturezaJuridica,
		QualificacaoResponsavel: data.QualificacaoResponsavel,
		CapitalSocial:           data.CapitalSocial,
		Porte:                   data.Porte,
	}

	return d

}

func structDataCnpjEmpresaToRes(data entity.CNPJEmpresa) *dadosReceitaPb.CnpjEmpresaData {

	dataSituacaoCadastral := ""

	if data.DataSituacaoCadastral != nil {
		dataSituacaoCadastral = *data.DataSituacaoCadastral
	}

	d := &dadosReceitaPb.CnpjEmpresaData{
		RazaoSocial:             data.RazaoSocial,
		IdEmpresa:               data.IdEmpresa,
		Subsidiaria:             data.Subsidiaria,
		CodigoVerificador:       data.CodigoVerificador,
		Cnpj:                    data.Cnpj,
		MatrizFilial:            data.MatrizFilial,
		Fantasia:                data.Fantasia,
		SituacaoCadastral:       data.SituacaoCadastral,
		DataSituacaoCadastral:   dataSituacaoCadastral,
		MotivoSituacaoCadastral: data.MotivoSituacaoCadastral,
		DataAbertura:            *data.DataAbertura,
		CnaePrincipal:           data.CnaePrincipal,
		CnaeSecundaria:          data.CnaeSecundaria,
		EnderecoTipoLogradouro:  data.EnderecoTipoLogradouro,
		EnderecoLogradouro:      data.EnderecoLogradouro,
		EnderecoNumero:          data.EnderecoNumero,
		EnderecoComplemento:     data.EnderecoComplemento,
		EnderecoBairro:          data.EnderecoBairro,
		EnderecoCep:             data.EnderecoCep,
		EnderecoUf:              data.EnderecoUf,
		EnderecoCodigoMunicipio: data.EnderecoCodigoMunicipio,
		Telefone1Ddd:            data.Telefone1Ddd,
		Telefone1Numero:         data.Telefone1Numero,
		Telefone2Ddd:            data.Telefone2Ddd,
		Telefone2Numero:         data.Telefone2Numero,
		FaxDdd:                  data.FaxDdd,
		FaxNumero:               data.FaxNumero,
		Email:                   data.Email,
		Id:                      data.Id,
		NomeMunicipio:           data.NomeMunicipio,
	}

	return d

}

func structDataSocioEmpresaToRes(data entity.SocioEmpresa) *dadosReceitaPb.SocioEmpresaData {

	d := &dadosReceitaPb.SocioEmpresaData{
		IdEmpresa:                data.IdEmpresa,
		TipoPessoa:               data.TipoPessoa,
		Nome:                     data.Nome,
		CpfCnpj:                  data.CpfCnpj,
		CodigoQualificacao:       data.CodigoQualificacao,
		Data:                     *data.Data,
		CpfRepresentanteLegal:    data.CpfRepresentanteLegal,
		NomeRepresentanteLegal:   data.NomeRepresentanteLegal,
		CodigoRepresentanteLegal: data.CodigoQualificacaoRepresentanteLegal,
		Id:                       data.Id,
		Codigo:                   data.Codigo,
		Descricao:                data.Descricao,
	}

	return d

}

func convertListEmpresaToProto(uD []entity.Empresa) []*dadosReceitaPb.EmpresaData {

	var listRes []*dadosReceitaPb.EmpresaData

	for _, d := range uD {

		listRes = append(listRes, structDataEmpresaToRes(d))

	}

	return listRes

}

func convertListCnpjEmpresaToProto(uD []entity.CNPJEmpresa) []*dadosReceitaPb.CnpjEmpresaData {

	var listRes []*dadosReceitaPb.CnpjEmpresaData

	for _, d := range uD {

		listRes = append(listRes, structDataCnpjEmpresaToRes(d))

	}

	return listRes

}

func convertListSocioEmpresaToProto(uD []entity.SocioEmpresa) []*dadosReceitaPb.SocioEmpresaData {

	var listRes []*dadosReceitaPb.SocioEmpresaData

	for _, d := range uD {

		listRes = append(listRes, structDataSocioEmpresaToRes(d))

	}

	return listRes

}

func ProcessCSVEmpresa() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}
	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE empresa;")
	for _, e := range entries {
		if strings.Index(e.Name(), "EMPRECSV") > 0 {
			handleCSVEmpresa(e.Name())
		}
	}

	db.Exec("OPTIMIZE TABLE empresa;")
}

func handleCSVEmpresa(fileName string) {
	//file, err := os.Open("data/output-extract/K3241.K03200Y9.D31111.EMPRECSV")
	file, err := os.Open("data/output-extract/" + fileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()

	//reader := csv.NewReader(file)
	reader := csv.NewReader(charmap.ISO8859_15.NewDecoder().Reader(file))

	reader.Comma = ';'
	reader.ReuseRecord = true

	var empresaList []*entity.Empresa
	db := dbService.GetDBConnection()
	for {
		record, err := reader.Read()

		if err == io.EOF {
			db.Table("empresa").CreateInBatches(empresaList, 10000)
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		i, err := strconv.ParseInt(record[5], 10, 32)
		if err != nil {
			i = 1
		}

		empresaList = append(empresaList, &entity.Empresa{
			Id:                      record[0],
			RazaoSocial:             record[1],
			CodigoNaturezaJuridica:  record[2],
			QualificacaoResponsavel: record[3],
			CapitalSocial:           strings.Replace(record[4], ",", ".", -1),
			Porte:                   int32(i),
		})

		if len(empresaList) == 100000 {
			db.Table("empresa").CreateInBatches(empresaList, 10000)
			empresaList = empresaList[:0] // slice with 0 length
		}

	}

	file.Close()
}
