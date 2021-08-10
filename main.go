package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

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
	filePaths, err := downloadFilesFromPackageList(year, packages)
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

func extractFiles(filePaths []string) ([]string, error) {
	var filenames []string
	for i, filename := range filePaths {
		r, err := zip.OpenReader(filename)
		if err != nil {
			return nil, fmt.Errorf("error while extracting zip files")
		}
		defer r.Close()
		for _, f := range r.File {
			fpath := filepath.Join(fmt.Sprintf("downloads/data/%d", i+1), f.Name)
			if !strings.HasPrefix(fpath, filepath.Clean(fmt.Sprintf("downloads/data/%d", i+1))+string(os.PathSeparator)) {
				return filenames, fmt.Errorf("%s: illegal file path", fpath)
			}
			filenames = append(filenames, fpath)
			if f.FileInfo().IsDir() {
				os.MkdirAll(fpath, os.ModePerm)
				continue
			}
			if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
				return filenames, err
			}
			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return filenames, err
			}
			rc, err := f.Open()
			if err != nil {
				return filenames, err
			}
			_, err = io.Copy(outFile, rc)
			outFile.Close()
			rc.Close()
			if err != nil {
				return filenames, err
			}
		}
	}
	return filenames, nil
}
