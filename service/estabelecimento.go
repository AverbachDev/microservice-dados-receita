package service

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strings"

	dadosReceitaPb "github.com/AverbachDev/grpc-nest-proto/proto"
	dbService "github.com/AverbachDev/microservice-dados-receita/db"
	"github.com/AverbachDev/microservice-dados-receita/entity"
	"github.com/AverbachDev/microservice-dados-receita/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/charmap"
	"gorm.io/gorm"
)

func ListEstabelecimento(data *dadosReceitaPb.ListCriteriaRequestEstabelecimento) (*dadosReceitaPb.ListResultEstabelecimento, int32, error) {
	var record []entity.Estabelecimento
	var result *dadosReceitaPb.ListResultEstabelecimento
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.Fantasia != "" {
		fields = append(fields, "fantasia LIKE ? ")
		values = append(values, "%"+data.Fantasia+"%")
	}

	if data.Cnpj != "" {
		fields = append(fields, "cnpj LIKE ? ")
		values = append(values, "%"+data.Cnpj+"%")
	}

	if err := db.Table("estabelecimento").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultEstabelecimento{
		Result: convertListEstabelecimentoToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateEstabelecimento(data *dadosReceitaPb.EstabelecimentoData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("estabelecimento").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataEstabelecimentoToRes(data entity.Estabelecimento) *dadosReceitaPb.EstabelecimentoData {

	d := &dadosReceitaPb.EstabelecimentoData{
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
	}

	return d

}

func convertListEstabelecimentoToProto(uD []entity.Estabelecimento) []*dadosReceitaPb.EstabelecimentoData {

	var listRes []*dadosReceitaPb.EstabelecimentoData

	for _, d := range uD {

		listRes = append(listRes, structDataEstabelecimentoToRes(d))

	}

	return listRes

}

func ProcessCSVEstabelecimento() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}
	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE estabelecimento;")
	for _, e := range entries {
		if strings.Index(e.Name(), "ESTABELE") > 0 {
			handleCSVEstabelecimento(e.Name())
		}
	}
	db.Exec("OPTIMIZE TABLE estabelecimento;")
}

func handleCSVEstabelecimento(fileName string) {
	//file, err := os.Open("data/output-extract/K3241.K03200Y9.D31111.ESTABELE")
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

	var estabelecimentoList []*entity.Estabelecimento

	for _, eachline := range records {

		matrizFilial := true
		if eachline[3] != "1" {
			matrizFilial = false
		}

		estabelecimentoList = append(estabelecimentoList, &entity.Estabelecimento{
			IdEmpresa:               eachline[0],
			Subsidiaria:             eachline[1],
			CodigoVerificador:       eachline[2],
			Cnpj:                    eachline[0] + eachline[1] + eachline[2],
			MatrizFilial:            matrizFilial,
			Fantasia:                eachline[4],
			SituacaoCadastral:       eachline[5],
			DataSituacaoCadastral:   utils.Parser_date(eachline[6]),
			MotivoSituacaoCadastral: eachline[7],
			DataAbertura:            utils.Parser_date(eachline[10]),
			CnaePrincipal:           utils.Parse_cnae(eachline[11]),
			CnaeSecundaria:          utils.Parse_cnae(eachline[12]),
			EnderecoTipoLogradouro:  eachline[13],
			EnderecoLogradouro:      eachline[14],
			EnderecoNumero:          eachline[15],
			EnderecoComplemento:     eachline[16],
			EnderecoBairro:          eachline[17],
			EnderecoCep:             eachline[18],
			EnderecoUf:              eachline[19],
			EnderecoCodigoMunicipio: eachline[20],
			Telefone1Ddd:            utils.Parse_ddd(eachline[21]),
			Telefone1Numero:         eachline[22],
			Telefone2Ddd:            utils.Parse_ddd(eachline[23]),
			Telefone2Numero:         eachline[24],
			FaxDdd:                  utils.Parse_ddd(eachline[25]),
			FaxNumero:               eachline[26],
			Email:                   eachline[27],
			Id:                      0,
		})
	}

	reader = nil
	db := dbService.GetDBConnection()

	db.Table("estabelecimento").CreateInBatches(estabelecimentoList, 300)
	defer clearListEstabelecimento(estabelecimentoList)

}

func clearListEstabelecimento(estabelecimentoList []*entity.Estabelecimento) {
	if estabelecimentoList != nil {
		estabelecimentoList = nil
	}
}
