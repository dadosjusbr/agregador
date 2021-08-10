package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

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
	packages, err := getBackupData(2020, "mppb")
	if err != nil {
		log.Fatal(err)
	}
	filePaths, err := downloadFilesFromPackageList(2020, packages)
	if err != nil {
		log.Fatal(err)
	}
	extractFiles(filePaths)
	fmt.Println("arquivos baixados")
}
func getBackupData(year int, agency string) ([]storage.Backup, error) {
	agenciesMonthlyInfo, err := client.Db.GetMonthlyInfoSummary([]storage.Agency{{ID: agency}}, year)
	if err != nil {
		return nil, fmt.Errorf("error fetching data: %v", err)
	}
	var packages []storage.Backup
	for _, agencyMonthlyInfo := range agenciesMonthlyInfo[agency] {
		if agencyMonthlyInfo.Package != nil {
			packages = append(packages, storage.Backup{URL: agencyMonthlyInfo.Package.URL, Hash: agencyMonthlyInfo.Package.Hash})
		}
	}
	return packages, nil
}

func downloadFilesFromPackageList(year int, list []storage.Backup) ([]string, error) {
	var paths []string
	for index, el := range list {
		filepath := fmt.Sprintf("downloads/%d-%d.zip", year, index+1)
		err := download(filepath, el.URL)
		if err != nil {
			return nil, fmt.Errorf("error while downloading files")
	}
		paths = append(paths, filepath)
	}
	return paths, nil
}

func download(filepath string, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	return err
}
