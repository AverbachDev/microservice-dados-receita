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
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/charmap"
	"gorm.io/gorm"
)

func ListQualificacaoSocio(data *dadosReceitaPb.ListCriteriaRequestQualificacaoSocio) (*dadosReceitaPb.ListResultQualificacaoSocio, int32, error) {
	var record []entity.QualificacaoSocio
	var result *dadosReceitaPb.ListResultQualificacaoSocio
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.Codigo != "" {
		fields = append(fields, "codigo LIKE ? ")
		values = append(values, "%"+data.Codigo+"%")
	}

	if data.Descricao != "" {
		fields = append(fields, "descricao LIKE ? ")
		values = append(values, "%"+data.Descricao+"%")
	}

	if err := db.Table("qualificacao-socio").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultQualificacaoSocio{
		Result: convertListQualificacaoSocioToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateQualificacaoSocio(data *dadosReceitaPb.QualificacaoSocioData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("qualificacao-socio").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataQualificacaoSocioToRes(data entity.QualificacaoSocio) *dadosReceitaPb.QualificacaoSocioData {

	d := &dadosReceitaPb.QualificacaoSocioData{
		Codigo:    data.Codigo,
		Descricao: data.Descricao,
	}

	return d

}

func convertListQualificacaoSocioToProto(uD []entity.QualificacaoSocio) []*dadosReceitaPb.QualificacaoSocioData {

	var listRes []*dadosReceitaPb.QualificacaoSocioData

	for _, d := range uD {

		listRes = append(listRes, structDataQualificacaoSocioToRes(d))

	}

	return listRes

}

func ProcessCSVQualificacaoSocio() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range entries {
		if strings.Index(e.Name(), "QUALSCSV") > 0 {
			handleCSVQualificacaoSocio(e.Name())
		}
	}
}

func handleCSVQualificacaoSocio(fileName string) {
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

	var qualificacaosocioList []*entity.QualificacaoSocio

	for _, eachline := range records {

		qualificacaosocioList = append(qualificacaosocioList, &entity.QualificacaoSocio{
			Codigo:    eachline[0],
			Descricao: eachline[1],
		})
	}

	reader = nil
	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE qualificacao_socio;")
	db.Table("qualificacao_socio").CreateInBatches(qualificacaosocioList, 10000)
	defer clearListQuali(qualificacaosocioList)

	db.Exec("OPTIMIZE TABLE qualificacao_socio;")
}

func clearListQuali(qualificacaosocioList []*entity.QualificacaoSocio) {
	if qualificacaosocioList != nil {
		qualificacaosocioList = nil
	}
}
