package service

import (
	"encoding/csv"
	"errors"
	"fmt"
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

	d := &dadosReceitaPb.CnpjEmpresaData{
		RazaoSocial:             data.RazaoSocial,
		IdEmpresa:               data.IdEmpresa,
		Subsidiaria:             data.Subsidiaria,
		CodigoVerificador:       data.CodigoVerificador,
		Cnpj:                    data.Cnpj,
		MatrizFilial:            data.MatrizFilial,
		Fantasia:                data.Fantasia,
		SituacaoCadastral:       data.SituacaoCadastral,
		DataSituacaoCadastral:   *data.DataSituacaoCadastral,
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
	records, err := reader.ReadAll()

	if err != nil {
		fmt.Println(err)
		return
	}

	var empresaList []*entity.Empresa

	for _, eachline := range records {

		i, err := strconv.ParseInt(eachline[5], 10, 32)
		if err != nil {
			i = 1
		}

		empresaList = append(empresaList, &entity.Empresa{
			Id:                      eachline[0],
			RazaoSocial:             eachline[1],
			CodigoNaturezaJuridica:  eachline[2],
			QualificacaoResponsavel: eachline[3],
			CapitalSocial:           strings.Replace(eachline[4], ",", ".", -1),
			Porte:                   int32(i),
		})
	}

	reader = nil
	db := dbService.GetDBConnection()
	db.Table("empresa").CreateInBatches(empresaList, 10000)
	defer clearListEmpresa(empresaList)
}

func clearListEmpresa(empresaList []*entity.Empresa) {
	if empresaList != nil {
		empresaList = nil
	}
}
