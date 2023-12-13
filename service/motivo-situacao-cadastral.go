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

func ListMotivoSituacaoCadastral(data *dadosReceitaPb.ListCriteriaRequestMotivoSituacaoCadastral) (*dadosReceitaPb.ListResultMotivoSituacaoCadastral, int32, error) {
	var record []entity.MotivoSituacaoCadastral
	var result *dadosReceitaPb.ListResultMotivoSituacaoCadastral
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

	if err := db.Table("motivo_situacao_cadastral").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultMotivoSituacaoCadastral{
		Result: convertListMotivoSituacaoCadastralToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateMotivoSituacaoCadastral(data *dadosReceitaPb.MotivoSituacaoCadastralData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("motivo_situacao_cadastral").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataMotivoSituacaoCadastralToRes(data entity.MotivoSituacaoCadastral) *dadosReceitaPb.MotivoSituacaoCadastralData {

	d := &dadosReceitaPb.MotivoSituacaoCadastralData{
		Codigo:    data.Codigo,
		Descricao: data.Descricao,
	}

	return d

}

func convertListMotivoSituacaoCadastralToProto(uD []entity.MotivoSituacaoCadastral) []*dadosReceitaPb.MotivoSituacaoCadastralData {

	var listRes []*dadosReceitaPb.MotivoSituacaoCadastralData

	for _, d := range uD {

		listRes = append(listRes, structDataMotivoSituacaoCadastralToRes(d))

	}

	return listRes

}

func ProcessCSVMotivoSituacaoCadastral() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE motivo_situacao_cadastral;")
	for _, e := range entries {
		if strings.Index(e.Name(), "MOTICSV") > 0 {
			handleCSVMotivoSituacaoCadastral(e.Name())
		}
	}
}

func handleCSVMotivoSituacaoCadastral(fileName string) {
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

	var motivosituacaocadastralList []*entity.MotivoSituacaoCadastral

	for _, eachline := range records {

		motivosituacaocadastralList = append(motivosituacaocadastralList, &entity.MotivoSituacaoCadastral{
			Codigo:    eachline[0],
			Descricao: eachline[1],
		})
	}

	reader = nil
	db := dbService.GetDBConnection()

	db.Table("motivo_situacao_cadastral").CreateInBatches(motivosituacaocadastralList, 10000)
	defer clearListMotivo(motivosituacaocadastralList)

	db.Exec("OPTIMIZE TABLE motivo_situacao_cadastral;")
}

func clearListMotivo(motivosituacaocadastralList []*entity.MotivoSituacaoCadastral) {
	if motivosituacaocadastralList != nil {
		motivosituacaocadastralList = nil
	}
}
