package main

import (
	"fmt"
	"log"

	"github.com/dadosjusbr/storage"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	MongoURI    string `envconfig:"MONGODB_URI" required:"true"`
	MongoDBName string `envconfig:"MONGODB_NAME" required:"true"`
	MongoMICol  string `envconfig:"MONGODB_MICOL" required:"true"`
	MongoAgCol  string `envconfig:"MONGODB_AGCOL" required:"true"`
}

var conf config
var client *storage.Client

func newClient(c config) (*storage.Client, error) {
	if c.MongoMICol == "" || c.MongoAgCol == "" {
		return nil, fmt.Errorf("error creating storage client: db collections must not be empty. MI:\"%s\", AG:\"%s\"", c.MongoMICol, c.MongoAgCol)
	}
	db, err := storage.NewDBClient(c.MongoURI, c.MongoDBName, c.MongoMICol, c.MongoAgCol)
	if err != nil {
		return nil, fmt.Errorf("error creating DB client: %q", err)
	}
	db.Collection(c.MongoMICol)
	client, err := storage.NewClient(db, &storage.CloudClient{})
	if err != nil {
		return nil, fmt.Errorf("error creating storage.client: %q", err)
	}
	return client, nil
}

func main() {
	godotenv.Load()
	err := envconfig.Process("remuneracao-magistrados", &conf)
	if err != nil {
		log.Fatal(err)
	}
	client, err = newClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	packages, err := getBackupData(2021, "mppb")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(packages)
}
func getBackupData(year int, agency string) ([]storage.Backup, error) {
	agenciesMonthlyInfo, err := client.Db.GetMonthlyInfo([]storage.Agency{{ID: "mppb"}}, 2020)
	if err != nil {
		return nil, fmt.Errorf("error fetching data")
	}
	var packages []storage.Backup
	for _, agencyMonthlyInfo := range agenciesMonthlyInfo["mppb"] {
		if agencyMonthlyInfo.Summary.MemberActive.Wage.Total+agencyMonthlyInfo.Summary.MemberActive.Perks.Total+agencyMonthlyInfo.Summary.MemberActive.Others.Total > 0 {
			if agencyMonthlyInfo.Package != nil {
				packages = append(packages, storage.Backup{URL: agencyMonthlyInfo.Package.URL, Hash: agencyMonthlyInfo.Package.Hash})
			}
		}
	}
	return packages, nil
}
