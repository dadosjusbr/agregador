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
	var agency string
	var outDir string
	flag.IntVar(&year, "year", 2018, "a year in which you want to collect monthly information")
	flag.StringVar(&agency, "agency", "", "an agency in which you want to collect monthly information")
	flag.StringVar(&outDir, "outDir", "out", "the output directory")
	flag.Parse()
	if agency == "" {
		log.Fatalf("missing flag agency")
	}
	godotenv.Load()
	err := envconfig.Process("remuneracao-magistrados", &conf)
	if err != nil {
		log.Fatal(err)
	}
	client, err = newClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	packages, err := getBackupData(year, agency)
	if err != nil {
		log.Fatal(err)
	}
	if err = os.MkdirAll(outDir, os.ModePerm); err != nil {
		log.Fatalf("error while creating new dir(%s): %q", outDir, err)
	}
	var csvList []string
	for _, p := range packages {
		if filepath.Ext(p.URL) == ".zip" {
			zFName := fmt.Sprintf("%d_%d_%s.zip", year, p.Month, agency)
			zPath := filepath.Join(outDir, zFName)
			if err := download(zPath, p.URL); err != nil {
				log.Fatal(err)
			}
			fmt.Println("arquivo baixado:", zPath)
			csvFName := fmt.Sprintf("%d_%d_%s.csv", year, p.Month, agency)
			csvPath := filepath.Join(outDir, csvFName)
			if err := unzip(zPath, csvPath); err != nil {
				log.Fatal(err)
			}
			fmt.Println("arquivo descompactado:", csvPath)
			csvList = append(csvList, csvPath)
			if err := os.Remove(zPath); err != nil {
				log.Fatal(err)
			}
			fmt.Println("arquivo zip apagado:", zPath)
		}
	}
	joinPath := filepath.Join(outDir, "data.csv")
	if err := mergeMIData(csvList, joinPath); err != nil {
		log.Fatal(err)
	}
	fmt.Println("arquivo final criado:", joinPath)
}
func getBackupData(year int, agency string) ([]extractionData, error) {
	agenciesMonthlyInfo, err := client.Db.GetMonthlyInfo([]storage.Agency{{ID: agency}}, year)
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
func unzip(zipPath, csvPath string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("error while extracting zip files: %q", err)
	}
	defer r.Close()
	for _, f := range r.File {
		// search for the file data.csv inside the zip files
		if f.Name == "data.csv" {
			zipContent, err := f.Open()
			if err != nil {
				return fmt.Errorf("error while opening file stream inside zip: %q", err)
			}
			err = func() error {
				out, err := os.Create(csvPath)
				if err != nil {
					return fmt.Errorf("error while creating new file(%s): %q", csvPath, err)
				}
				defer out.Close()
				_, err = io.Copy(out, zipContent)
				if err != nil {
					return fmt.Errorf("error while filling file stream outside zip: %q", err)
				}
				return nil
			}()
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
}
func mergeMIData(filePaths []string, joinPath string) error {
	var finalCsv [][]string
	for _, f := range filePaths {
		csvLines, err := func() ([][]string, error) {
			csvFile, err := os.Open(f)
			if err != nil {
				return nil, err
			}
			defer csvFile.Close()
			return csv.NewReader(csvFile).ReadAll()
		}()
		if err != nil {
			return fmt.Errorf("error while reading csv file: %q", err)
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
	}
	finalCsvFile, err := os.Create(joinPath)
	if err != nil {
		return fmt.Errorf("failed creating file: %q", err)
	}
	csvwriter := csv.NewWriter(finalCsvFile)
	for _, empRow := range finalCsv {
		if err := csvwriter.Write(empRow); err != nil {
			return fmt.Errorf("error writing csv file: %q", err)
		}
	}
	csvwriter.Flush()
	finalCsvFile.Close()
	return nil
}
