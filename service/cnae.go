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

func ListCnae(data *dadosReceitaPb.ListCriteriaRequestCnae) (*dadosReceitaPb.ListResultCnae, int32, error) {
	var record []entity.Cnae
	var result *dadosReceitaPb.ListResultCnae
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.Cnae != "" {
		fields = append(fields, "cnae LIKE ? ")
		values = append(values, "%"+data.Cnae+"%")
	}

	if data.Descricao != "" {
		fields = append(fields, "descricao LIKE ? ")
		values = append(values, "%"+data.Descricao+"%")
	}

	if err := db.Table("cnae").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultCnae{
		Result: convertListToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateCnae(data *dadosReceitaPb.CnaeData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("cnae").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataToRes(data entity.Cnae) *dadosReceitaPb.CnaeData {

	d := &dadosReceitaPb.CnaeData{
		Cnae:      data.Cnae,
		Descricao: data.Descricao,
	}

	return d

}

func convertListToProto(uD []entity.Cnae) []*dadosReceitaPb.CnaeData {

	var listRes []*dadosReceitaPb.CnaeData

	for _, d := range uD {

		listRes = append(listRes, structDataToRes(d))

	}

	return listRes

}

func ProcessCSVCnae() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE cnae;")
	for _, e := range entries {
		if strings.Index(e.Name(), "CNAECSV") > 0 {
			handleCSV(e.Name())
		}
	}
}

func handleCSV(fileName string) {
	//file, err := os.Open("data/output-extract/F.K03200$Z.D31111.CNAECSV")
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

	var cnaeList []*entity.Cnae

	for _, eachline := range records {
		cnaeList = append(cnaeList, &entity.Cnae{
			Cnae:      eachline[0],
			Descricao: eachline[1],
		})
	}

	reader = nil
	db := dbService.GetDBConnection()

	db.Table("cnae").CreateInBatches(cnaeList, 10000)

	defer clearListCnae(cnaeList)

	db.Exec("OPTIMIZE TABLE cnae;")
}

func clearListCnae(cnaeList []*entity.Cnae) {
	if cnaeList != nil {
		cnaeList = nil
	}
}
