package main

import (
	"archive/zip"
	"encoding/csv"
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
	filePaths, err := downloadFilesFromPackageList(packages)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("arquivos baixados")
	extractedFilePaths, err := extractFiles(filePaths)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("arquivos extraidos")
	mergeMIData(year, extractedFilePaths)
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

func download(fp string, url string) error {
	os.MkdirAll(filepath.Dir(fp), 0700)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(fp)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func extractFiles(filePaths []string) ([]string, error) {
	var filenames []string
	for _, filename := range filePaths {
		// open zip reader to extract files
		r, err := zip.OpenReader(filename)
		if err != nil {
			return nil, fmt.Errorf("error while extracting zip files")
		}
		for _, f := range r.File {
			// search for the file data.csv inside the zip files
			if f.Name == "data.csv" {
				fpath := filepath.Join(fmt.Sprint(filepath.Dir(filename)), f.Name)
				if !strings.HasPrefix(fpath, filepath.Clean(fmt.Sprint(filepath.Dir(filename)))+string(os.PathSeparator)) {
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
				out, err := os.Create(fpath)
				if err != nil {
					return nil, err
				}
				rc, err := f.Open()
				if err != nil {
					return filenames, err
				}
				_, err = io.Copy(out, rc)
				if err != nil {
					return nil, err
				}
				if err != nil {
					return filenames, err
				}
				out.Close()
				break
			}
		}
		r.Close()
	}
	return filenames, nil
}
func mergeMIData(year int, filePaths []string) error {
	var finalCsv [][]string
	for _, f := range filePaths {
		csvFile, err := os.Open(f)
		if err != nil {
			return fmt.Errorf("error while reading csv file")
		}
		csvLines, err := csv.NewReader(csvFile).ReadAll()
		if err != nil {
			return fmt.Errorf("error while reading csv data")
		}
		for i, line := range csvLines {
			if i != 0 {
				if line[0] != "aid" {
					finalCsv = append(finalCsv, line)
				}
			} else {
				finalCsv = append(finalCsv, line)
			}
		}
		csvFile.Close()
	}
	finalCsvFile, err := os.Create("downloads/test.csv")
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	csvwriter := csv.NewWriter(finalCsvFile)
	for _, empRow := range finalCsv {
		_ = csvwriter.Write(empRow)
	}
	csvwriter.Flush()
	finalCsvFile.Close()
	return nil
}
