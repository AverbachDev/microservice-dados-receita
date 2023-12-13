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

func ListPais(data *dadosReceitaPb.ListCriteriaRequestPais) (*dadosReceitaPb.ListResultPais, int32, error) {
	var record []entity.Pais
	var result *dadosReceitaPb.ListResultPais
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

	if err := db.Table("pais").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultPais{
		Result: convertListPaisToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreatePais(data *dadosReceitaPb.PaisData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("pais").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataPaisToRes(data entity.Pais) *dadosReceitaPb.PaisData {

	d := &dadosReceitaPb.PaisData{
		Codigo:    data.Codigo,
		Descricao: data.Descricao,
	}

	return d

}

func convertListPaisToProto(uD []entity.Pais) []*dadosReceitaPb.PaisData {

	var listRes []*dadosReceitaPb.PaisData

	for _, d := range uD {

		listRes = append(listRes, structDataPaisToRes(d))

	}

	return listRes

}

func ProcessCSVPais() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range entries {
		if strings.Index(e.Name(), "PAISCSV") > 0 {
			handleCSVPais(e.Name())
		}
	}
}

func handleCSVPais(fileName string) {
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

	var paisList []*entity.Pais

	for _, eachline := range records {

		paisList = append(paisList, &entity.Pais{
			Codigo:    eachline[0],
			Descricao: eachline[1],
		})
	}

	reader = nil
	db := dbService.GetDBConnection()

	db.Exec("TRUNCATE pais;")
	db.Table("pais").CreateInBatches(paisList, 10000)

	defer clearListPais(paisList)

	db.Exec("OPTIMIZE TABLE pais;")
}

func clearListPais(paisList2 []*entity.Pais) {
	if paisList2 != nil {
		paisList2 = nil
	}
}
