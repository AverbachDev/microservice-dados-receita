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

func ListOptanteSimples(data *dadosReceitaPb.ListCriteriaRequestOptanteSimples) (*dadosReceitaPb.ListResultOptanteSimples, int32, error) {
	var record []entity.OptanteSimples
	var result *dadosReceitaPb.ListResultOptanteSimples
	db := dbService.GetDBConnection()

	fields := []string{}
	values := []interface{}{}

	if data.IdEmpresa != "" {
		fields = append(fields, "id_empresa LIKE ? ")
		values = append(values, "%"+data.IdEmpresa+"%")
	}

	if data.Simples != "" {
		fields = append(fields, "simples LIKE ? ")
		values = append(values, "%"+data.Simples+"%")
	}

	if err := db.Table("optante_simples").Where(strings.Join(fields, " AND "), values...).Find(&record).Error; err != nil {
		log.Info("failure", []entity.Cnae{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return result, 404, fmt.Errorf("Cnae not found")
		}
		return result, 400, fmt.Errorf("failed to get blog: %w", err)
	}
	if len(record) == 0 {
		return result, 404, gorm.ErrRecordNotFound
	}

	return &dadosReceitaPb.ListResultOptanteSimples{
		Result: convertListOptanteSimplesToProto(record),
		Total:  int32(len(record)),
	}, 200, nil

}

func CreateOptanteSimples(data *dadosReceitaPb.OptanteSimplesData) error {

	db := dbService.GetDBConnection()

	if err := db.Table("optante_simples").Create(&data).Error; err != nil {
		log.Info("failure", dadosReceitaPb.CnaeData{}, err)
	}

	return nil

}

func structDataOptanteSimplesToRes(data entity.OptanteSimples) *dadosReceitaPb.OptanteSimplesData {

	d := &dadosReceitaPb.OptanteSimplesData{
		IdEmpresa:     data.IdEmpresa,
		Simples:       data.Simples,
		SimplesInicio: *data.SimplesInicio,
		SimplesFim:    *data.SimplesFim,
		Simei:         data.Simei,
		SimeiInicio:   *data.SimeiInicio,
		SimeiFim:      *data.SimeiFim,
	}

	return d

}

func convertListOptanteSimplesToProto(uD []entity.OptanteSimples) []*dadosReceitaPb.OptanteSimplesData {

	var listRes []*dadosReceitaPb.OptanteSimplesData

	for _, d := range uD {

		listRes = append(listRes, structDataOptanteSimplesToRes(d))

	}

	return listRes

}

func ProcessCSVOptanteSimples() {
	entries, err := os.ReadDir("data/output-extract/")
	if err != nil {
		log.Fatal(err)
	}

	db := dbService.GetDBConnection()
	db.Exec("TRUNCATE optante_simples;")
	for _, e := range entries {
		if strings.Index(e.Name(), "SIMPLES") > 0 {
			handleCSVOptanteSimples(e.Name())
		}
	}

	db.Exec("OPTIMIZE TABLE optante_simples;")
}

func handleCSVOptanteSimples(fileName string) {
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

	chunk := 1000000
	loopTimes := len(records) / chunk

	db := dbService.GetDBConnection()

	for i := 0; i < loopTimes; i++ {
		initialPositionSlice := 0
		endPositionSlice := chunk
		if i > 0 {
			initialPositionSlice = i * chunk
			endPositionSlice = (i * chunk) + chunk
			if endPositionSlice > len(records) {
				endPositionSlice = len(records)
			}
		}

		var optantesimplesList []*entity.OptanteSimples
		for j := initialPositionSlice; j < endPositionSlice; j++ {
			optantesimplesList = append(optantesimplesList, &entity.OptanteSimples{
				IdEmpresa:     records[j][0],
				Simples:       records[j][1],
				SimplesInicio: utils.Parser_date(records[j][2]),
				SimplesFim:    utils.Parser_date(records[j][3]),
				Simei:         records[j][4],
				SimeiInicio:   utils.Parser_date(records[j][5]),
				SimeiFim:      utils.Parser_date(records[j][6]),
			})
		}

		if err := db.Table("optante_simples").CreateInBatches(optantesimplesList, 1000).Error; err != nil {
			panic(err)
		}
		log.Info("batch: ", initialPositionSlice)
	}
}
