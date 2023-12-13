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

func ListSocio(data *dadosReceitaPb.ListCriteriaRequestSocio) (*dadosReceitaPb.ListResultSocio, int32, error) {
	var record []entity.Socio
	var result *dadosReceitaPb.ListResultSocio
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.CpfCnpj != "" {
		fields = append(fields, "cpf_cnpj LIKE ? ")
		values = append(values, "%"+data.CpfCnpj+"%")
	}

	if data.IdEmpresa != "" {
		fields = append(fields, "id_empresa LIKE ? ")
		values = append(values, "%"+data.IdEmpresa+"%")
	}

	if err := db.Table("socio").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultSocio{
		Result: convertListSocioToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateSocio(data *dadosReceitaPb.SocioData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("socio").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataSocioToRes(data entity.Socio) *dadosReceitaPb.SocioData {

	d := &dadosReceitaPb.SocioData{
		IdEmpresa:                            data.IdEmpresa,
		TipoPessoa:                           data.TipoPessoa,
		Nome:                                 data.Nome,
		CpfCnpj:                              data.CpfCnpj,
		CodigoQualificacao:                   data.CodigoQualificacao,
		Data:                                 *data.Data,
		CpfRepresentanteLegal:                data.CpfRepresentanteLegal,
		NomeRepresentanteLegal:               data.NomeRepresentanteLegal,
		CodigoQualificacaoRepresentanteLegal: data.CodigoQualificacaoRepresentanteLegal,
		Id:                                   0,
	}

	return d

}

func convertListSocioToProto(uD []entity.Socio) []*dadosReceitaPb.SocioData {

	var listRes []*dadosReceitaPb.SocioData

	for _, d := range uD {

		listRes = append(listRes, structDataSocioToRes(d))

	}

	return listRes

}

func ProcessCSVSocio() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE socio;")
	for _, e := range entries {
		if strings.Index(e.Name(), "SOCIOCSV") > 0 {
			handleCSVSocio(e.Name())
		}
	}

	db.Exec("OPTIMIZE TABLE socio;")
}

func handleCSVSocio(fileName string) {
	//file, err := os.Open("data/output-extract/K3241.K03200Y9.D31111.EMPRECSV")
	file, err := os.Open("data/output-extract/" + fileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()

	reader := csv.NewReader(charmap.ISO8859_15.NewDecoder().Reader(file))

	reader.Comma = ';'
	records, err := reader.ReadAll()

	if err != nil {
		fmt.Println(err)
		return
	}

	var socioList []*entity.Socio

	for _, eachline := range records {

		tipoPessoa := true
		if eachline[3] != "1" {
			tipoPessoa = false
		}
		socioList = append(socioList, &entity.Socio{
			IdEmpresa:                            eachline[0],
			TipoPessoa:                           tipoPessoa,
			Nome:                                 eachline[2],
			CpfCnpj:                              eachline[3],
			CodigoQualificacao:                   eachline[4],
			Data:                                 utils.Parser_date(eachline[5]),
			CpfRepresentanteLegal:                eachline[7],
			NomeRepresentanteLegal:               eachline[8],
			CodigoQualificacaoRepresentanteLegal: eachline[9],
			Id:                                   0,
		})
	}

	reader = nil
	db := dbService.GetDBConnection()

	db.Table("socio").CreateInBatches(socioList, 5000)

	defer clearListSocio(socioList)

}

func clearListSocio(socioList []*entity.Socio) {
	if socioList != nil {
		socioList = nil
	}

}
