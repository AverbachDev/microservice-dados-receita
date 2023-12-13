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

func ListMunicipio(data *dadosReceitaPb.ListCriteriaRequestMunicipio) (*dadosReceitaPb.ListResultMunicipio, int32, error) {
	var record []entity.Municipio
	var result *dadosReceitaPb.ListResultMunicipio
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.Codigo != "" {
		fields = append(fields, "codigo LIKE ? ")
		values = append(values, "%"+data.Codigo+"%")
	}

	if data.Nome != "" {
		fields = append(fields, "nome LIKE ? ")
		values = append(values, "%"+data.Nome+"%")
	}

	if err := db.Table("municipio").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultMunicipio{
		Result: convertListMunicipioToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateMunicipio(data *dadosReceitaPb.MunicipioData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("municipio").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataMunicipioToRes(data entity.Municipio) *dadosReceitaPb.MunicipioData {

	d := &dadosReceitaPb.MunicipioData{
		Codigo: data.Codigo,
		Nome:   data.Nome,
	}

	return d

}

func convertListMunicipioToProto(uD []entity.Municipio) []*dadosReceitaPb.MunicipioData {

	var listRes []*dadosReceitaPb.MunicipioData

	for _, d := range uD {

		listRes = append(listRes, structDataMunicipioToRes(d))

	}

	return listRes

}

func ProcessCSVMunicipio() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range entries {
		if strings.Index(e.Name(), "MUNICCSV") > 0 {
			handleCSVMunicipio(e.Name())
		}
	}
}

func handleCSVMunicipio(fileName string) {
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

	var municipioList []*entity.Municipio

	for _, eachline := range records {

		municipioList = append(municipioList, &entity.Municipio{
			Codigo: eachline[0],
			Nome:   eachline[1],
		})
	}

	reader = nil
	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE municipio;")
	db.Table("municipio").CreateInBatches(municipioList, 10000)
	defer clearListMunicipio(municipioList)

	db.Exec("OPTIMIZE TABLE municipio;")
}

func clearListMunicipio(municipioList []*entity.Municipio) {
	if municipioList != nil {
		municipioList = nil
	}
}
