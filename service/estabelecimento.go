package service

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
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
			file, err := os.Open("data/output-extract/" + e.Name())
			if err != nil {
				fmt.Println(err)
				return
			}

			//reader := csv.NewReader(file)
			reader := csv.NewReader(charmap.ISO8859_15.NewDecoder().Reader(file))

			reader.ReuseRecord = true

			reader.Comma = ';'

			var estabelecimentoList []*entity.Estabelecimento
			for {
				record, err := reader.Read()
				if err == io.EOF {
					db.Table("estabelecimento").CreateInBatches(estabelecimentoList, 300)
					break
				}
				if err != nil {
					log.Fatal(err)
				}

				matrizFilial := true
				if record[3] != "1" {
					matrizFilial = false
				}

				estabelecimentoList = append(estabelecimentoList, &entity.Estabelecimento{
					IdEmpresa:               record[0],
					Subsidiaria:             record[1],
					CodigoVerificador:       record[2],
					Cnpj:                    record[0] + record[1] + record[2],
					MatrizFilial:            matrizFilial,
					Fantasia:                record[4],
					SituacaoCadastral:       record[5],
					DataSituacaoCadastral:   utils.Parser_date(record[6]),
					MotivoSituacaoCadastral: record[7],
					DataAbertura:            utils.Parser_date(record[10]),
					CnaePrincipal:           utils.Parse_cnae(record[11]),
					CnaeSecundaria:          utils.Parse_cnae(record[12]),
					EnderecoTipoLogradouro:  record[13],
					EnderecoLogradouro:      record[14],
					EnderecoNumero:          record[15],
					EnderecoComplemento:     record[16],
					EnderecoBairro:          record[17],
					EnderecoCep:             record[18],
					EnderecoUf:              record[19],
					EnderecoCodigoMunicipio: record[20],
					Telefone1Ddd:            utils.Parse_ddd(record[21]),
					Telefone1Numero:         record[22],
					Telefone2Ddd:            utils.Parse_ddd(record[23]),
					Telefone2Numero:         record[24],
					FaxDdd:                  utils.Parse_ddd(record[25]),
					FaxNumero:               record[26],
					Email:                   record[27],
					Id:                      0,
				})

				if len(estabelecimentoList) == 50000 {
					db.Table("estabelecimento").CreateInBatches(estabelecimentoList, 250)
					estabelecimentoList = estabelecimentoList[:0] // slice with 0 length
				}

			}

			file.Close()
		}
	}

	db.Exec("OPTIMIZE TABLE estabelecimento;")

}
