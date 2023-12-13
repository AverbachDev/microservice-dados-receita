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

func ListNaturezaJuridica(data *dadosReceitaPb.ListCriteriaRequestNaturezaJuridica) (*dadosReceitaPb.ListResultNaturezaJuridica, int32, error) {
	var record []entity.NaturezaJuridica
	var result *dadosReceitaPb.ListResultNaturezaJuridica
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

	if err := db.Table("natureza_juridica").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultNaturezaJuridica{
		Result: convertListNaturezaJuridicaToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateNaturezaJuridica(data *dadosReceitaPb.NaturezaJuridicaData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("natureza_juridica").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataNaturezaJuridicaToRes(data entity.NaturezaJuridica) *dadosReceitaPb.NaturezaJuridicaData {

	d := &dadosReceitaPb.NaturezaJuridicaData{
		Codigo:    data.Codigo,
		Descricao: data.Descricao,
	}

	return d

}

func convertListNaturezaJuridicaToProto(uD []entity.NaturezaJuridica) []*dadosReceitaPb.NaturezaJuridicaData {

	var listRes []*dadosReceitaPb.NaturezaJuridicaData

	for _, d := range uD {

		listRes = append(listRes, structDataNaturezaJuridicaToRes(d))

	}

	return listRes

}

func ProcessCSVNaturezaJuridica() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE natureza_juridica;")
	for _, e := range entries {
		if strings.Index(e.Name(), "NATJUCSV") > 0 {
			handleCSVNaturezaJuridica(e.Name())
		}
	}
}

func handleCSVNaturezaJuridica(fileName string) {
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

	var naturezajuridicaList []*entity.NaturezaJuridica

	for _, eachline := range records {

		naturezajuridicaList = append(naturezajuridicaList, &entity.NaturezaJuridica{
			Codigo:    eachline[0],
			Descricao: eachline[1],
		})
	}

	reader = nil
	db := dbService.GetDBConnection()

	db.Table("natureza_juridica").CreateInBatches(naturezajuridicaList, 10000)
	defer clearListNatureza(naturezajuridicaList)

	db.Exec("OPTIMIZE TABLE natureza_juridica;")
}

func clearListNatureza(naturezajuridicaList []*entity.NaturezaJuridica) {
	if naturezajuridicaList != nil {
		naturezajuridicaList = nil
	}
}
