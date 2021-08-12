package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

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

type extractionData struct {
	Year  int
	Month int
	URL   string
	Hash  string
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
	var year int
	flag.IntVar(&year, "year", 2018, "a year in which you want to collect monthly information")
	flag.Parse()
	godotenv.Load()
	err := envconfig.Process("remuneracao-magistrados", &conf)
	if err != nil {
		log.Fatal(err)
	}
	client, err = newClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	packages, err := getBackupData(year, "mppb")
	if err != nil {
		log.Fatal(err)
	}
	downloadFilesFromPackageList(packages)
	fmt.Println("arquivos baixados")
}
func getBackupData(year int, agency string) ([]extractionData, error) {
	agenciesMonthlyInfo, err := client.Db.GetMonthlyInfoSummary([]storage.Agency{{ID: agency}}, year)
	if err != nil {
		return nil, fmt.Errorf("error fetching data: %v", err)
	}
	var packages []extractionData
	for _, agencyMonthlyInfo := range agenciesMonthlyInfo[agency] {
		if agencyMonthlyInfo.Package != nil {
			packages = append(packages,
				extractionData{Year: agencyMonthlyInfo.Year,
					Month: agencyMonthlyInfo.Month,
					URL:   agencyMonthlyInfo.Package.URL,
					Hash:  agencyMonthlyInfo.Package.Hash})
		}
	}
	return packages, nil
}

func downloadFilesFromPackageList(list []extractionData) ([]string, error) {
	var paths []string
	for _, el := range list {
		if filepath.Ext(el.URL) == ".zip" {
			fp := fmt.Sprintf("downloads/%d/%d/package.zip", el.Year, el.Month)
			err := download(fp, el.URL)
			if err != nil {
				return nil, fmt.Errorf("error while downloading files")
			}
			paths = append(paths, fp)
		}
	}
	return paths, nil
}

func download(filepath string, url string) error {
	if err := os.MkdirAll(filepath, os.ModePerm); err != nil {
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
		if err != nil {
			return err
		}
	}
	return nil
}
