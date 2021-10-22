package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/dadosjusbr/storage"
	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	MongoURI       string `envconfig:"MONGODB_URI" required:"true"`
	MongoDBName    string `envconfig:"MONGODB_NAME" required:"true"`
	MongoMICol     string `envconfig:"MONGODB_MICOL" required:"true"`
	MongoAgCol     string `envconfig:"MONGODB_AGCOL" required:"true"`
	MongoPkgCol    string `envconfig:"MONGODB_PKGCOL" required:"true"`
	SwiftUsername  string `envconfig:"SWIFT_USERNAME" required:"true"`
	SwiftAPIKey    string `envconfig:"SWIFT_APIKEY" required:"true"`
	SwiftAuthURL   string `envconfig:"SWIFT_AUTHURL" required:"true"`
	SwiftDomain    string `envconfig:"SWIFT_DOMAIN" required:"true"`
	SwiftContainer string `envconfig:"SWIFT_CONTAINER" required:"true"`
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
	db, err := storage.NewDBClient(c.MongoURI, c.MongoDBName, c.MongoMICol, c.MongoAgCol, c.MongoPkgCol)
	if err != nil {
		return nil, fmt.Errorf("error creating DB client: %q", err)
	}
	db.Collection(c.MongoMICol)
	bc := storage.NewCloudClient(conf.SwiftUsername, conf.SwiftAPIKey, conf.SwiftAuthURL, conf.SwiftDomain, conf.SwiftContainer)
	client, err := storage.NewClient(db, bc)
	if err != nil {
		return nil, fmt.Errorf("error creating storage.client: %q", err)
	}
	return client, nil
}

const (
	packageFileName = "datapackage_descriptor.json" // name of datapackage descriptor
)

var csvFileNames = []string{
	"coleta",
	"contra_cheque",
	"remuneracao",
	"metadados"}

func main() {
	godotenv.Load()
	if err := envconfig.Process("remuneracao-magistrados", &conf); err != nil {
		log.Fatal(err)
	}
	var grop_by string
	var outDir string
	var year int
	var agency string
	var group string

	flag.StringVar(&grop_by, "group_by", "", "an grop_by in which you want to collect monthly information")
	flag.StringVar(&outDir, "outDir", "out", "the output directory")
	flag.StringVar(&agency, "agency", "", "the given agency to agreggate monthly information")
	flag.StringVar(&group, "group", "", "the given group to agreggate the agencies monthly information")
	flag.IntVar(&year, "year", 2018, "the agreggation given year")
	flag.Parse()
	if grop_by == "" {
		log.Fatalf("missing flag group_by")
	}
	c, err := newClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	client = c
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		log.Fatalf("error while creating new dir(%s): %q", outDir, err)
	}
	switch grop_by {
	case "agency/year":
		var agencies []storage.Agency
		if agency == "" {
			if agencies, err = client.Db.GetAllAgencies(); err != nil {
				log.Fatalf("error while indexing agencies: %q", err)
			}
		} else {
			ag, err := client.Db.GetAgency(agency)
			if err != nil {
				log.Fatalf("error while searching for the agency %s: %q", agency, err)
			}
			agencies = append(agencies, *ag)
		}
		if err := agregateDataByAgencyYear(year, outDir, agencies); err != nil {
			log.Fatalf("error while agreggating by agency/year: %q", err)
		}
	case "group/year":
		if group == "" {
			log.Fatalf("missing flag group")
		}
		if err := agregateDataByGroupYear(year, outDir, group); err != nil {
			log.Fatalf("error while agreggating by group/year: %q", err)
		}
	default:
		log.Fatalf("please, select some grouping to aggregate")
	}
	fmt.Printf("dados agregados!")
}

func agregateDataByAgencyYear(year int, outDir string, agencies []storage.Agency) error {
	for _, ag := range agencies {
		agency := ag.ID
		packages, err := getBackupData(year, agency)
		if err != nil {
			return err
		}
		var csvLists []map[string]string
		csvLists, err = getCsvListsByAgencyYear(packages, year, agency, outDir, csvLists)
		if err != nil {
			return err
		}
		if err := mergeMIData(csvLists, outDir); err != nil {
			return err
		}
		dataPackageFilename, err := createDataPackage(agency, year, packageFileName, outDir)
		if err != nil {
			return err
		}
		if err := saveAgencyByYearPackage(dataPackageFilename, year, &agency); err != nil {
			return err
		}
	}
	return nil
}

func agregateDataByGroupYear(year int, outDir string, group string) error {
	agencies, _, err := client.Db.GetOPE(group, year)
	if err != nil {
		return err
	}
	for _, ag := range agencies {
		agency := ag.ID
		packages, err := getBackupData(year, agency)
		if err != nil {
			return err
		}
		var csvLists []map[string]string
		csvLists, err = getCsvListByGroupYear(packages, year, group, agency, outDir, csvLists)
		if err != nil {
			return err
		}
		if err := mergeMIData(csvLists, outDir); err != nil {
			return err
		}
		dataPackageFilename, err := createDataPackage(agency, year, packageFileName, outDir)
		if err != nil {
			return err
		}
		if err := saveGroupByYearPackage(dataPackageFilename, year, &group); err != nil {
			return err
		}
	}
	return nil
}

func saveAgencyByYearPackage(dataPackageFilename string, year int, agency *string) error {
	fmt.Println("arquivo final criado:", dataPackageFilename)
	packBackup, err := client.Cloud.UploadFile(dataPackageFilename, *agency)
	if err != nil {
		return err
	}
	if err := client.StorePackage(storage.Package{
		AgencyID: agency,
		Year:     &year,
		Month:    nil,
		Group:    nil,
		Package:  *packBackup}); err != nil {
		return err
	}
	fmt.Println("arquivo de backup criado", packBackup)
	return nil
}
func saveGroupByYearPackage(dataPackageFilename string, year int, group *string) error {
	fmt.Println("arquivo final criado:", dataPackageFilename)
	packBackup, err := client.Cloud.UploadFile(dataPackageFilename, *group)
	if err != nil {
		return err
	}
	if err := client.StorePackage(storage.Package{
		AgencyID: nil,
		Year:     &year,
		Month:    nil,
		Group:    group,
		Package:  *packBackup}); err != nil {
		return err
	}
	fmt.Println("arquivo de backup criado", packBackup)
	return nil
}

func getCsvListsByAgencyYear(packages []extractionData, year int, agency string, outDir string, csvLists []map[string]string) ([]map[string]string, error) {
	for _, p := range packages {
		if filepath.Ext(p.URL) == ".zip" {
			zFName := fmt.Sprintf("%d_%d_%s.zip", year, p.Month, agency)
			zPath := filepath.Join(outDir, zFName)
			fmt.Println("arquivo baixado:", zPath)
			if err := download(zPath, p.URL); err != nil {
				return nil, err
			}
			csvFName := fmt.Sprintf("%d_%d_%s.csv", year, p.Month, agency)
			extractedFiles, err := unzip(zPath, csvFName, outDir)
			if err != nil {
				return nil, err
			}
			for _, f := range extractedFiles {
				fmt.Println("arquivo descompactado:", f)
			}
			csvLists = append(csvLists, extractedFiles)
			if err := os.Remove(zPath); err != nil {
				return nil, err
			}
			fmt.Println("arquivo zip apagado:", zPath)
		}
	}
	return csvLists, nil
}

func getCsvListByGroupYear(packages []extractionData, year int, group string, agency string, outDir string, csvLists []map[string]string) ([]map[string]string, error) {
	for _, p := range packages {
		if filepath.Ext(p.URL) == ".zip" {
			zFName := fmt.Sprintf("%d_%d_%s_%s.zip", year, p.Month, agency, group)
			zPath := filepath.Join(outDir, zFName)
			if err := download(zPath, p.URL); err != nil {
				return nil, err
			}
			fmt.Println("arquivo baixado:", zPath)
			csvFName := fmt.Sprintf("%d_%d_%s_%s.csv", year, p.Month, agency, group)
			csvPath := filepath.Join(outDir, csvFName)
			extractedFiles, err := unzip(zPath, csvPath, outDir)
			if err != nil {
				return nil, err
			}
			for _, f := range extractedFiles {
				fmt.Println("arquivo descompactado:", f)
			}
			csvLists = append(csvLists, extractedFiles)
			if err := os.Remove(zPath); err != nil {
				return nil, err
			}
			fmt.Println("arquivo zip apagado:", zPath)
		}
	}
	return csvLists, nil
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
func unzip(zipPath, csvFName string, outDir string) (map[string]string, error) {
	extractedFiles := make(map[string]string)
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("error while extracting zip files: %q", err)
	}
	defer r.Close()
	for _, f := range r.File {
		// search for the file data.csv inside the zip files
		for _, csvFile := range csvFileNames {
			if f.Name == fmt.Sprintf("%s.csv", csvFile) {
				zipContent, err := f.Open()
				if err != nil {
					return nil, fmt.Errorf("error while opening file stream inside zip: %q", err)
				}
				err = func() error {
					csvFileName := filepath.Join(outDir, fmt.Sprintf("%s_%s", csvFile, csvFName))
					out, err := os.Create(csvFileName)
					if err != nil {
						return fmt.Errorf("error while creating new file(%s): %q", csvFileName, err)
					}
					defer out.Close()
					_, err = io.Copy(out, zipContent)
					if err != nil {
						return fmt.Errorf("error while filling file stream outside zip: %q", err)
					}
					extractedFiles[csvFile] = csvFileName
					return nil
				}()
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}
	return extractedFiles, nil
}
func createDataPackage(agency string, year int, packageFileName string, outDir string) (string, error) {
	c, err := ioutil.ReadFile(packageFileName)
	if err != nil {
		return "", fmt.Errorf("error reading datapackge_descriptor.json:%q", err)
	}
	var desc map[string]interface{}
	if err := json.Unmarshal(c, &desc); err != nil {
		return "", fmt.Errorf("error unmarshaling datapackage descriptor:%q", err)
	}
	desc["aid"] = agency
	desc["year"] = year
	pkg, err := datapackage.New(desc, outDir)
	if err != nil {
		return "", fmt.Errorf("error create datapackage:%q", err)
	}
	zipName := filepath.Join(outDir, fmt.Sprintf("%s-%d.zip", agency, year))
	if err := pkg.Zip(zipName); err != nil {
		return "", fmt.Errorf("error zipping datapackage (%s:%q)", zipName, err)
	}
	for _, csvFile := range csvFileNames {
		if err := os.Remove(filepath.Join(outDir, fmt.Sprintf("%s.csv", csvFile))); err != nil {
			return "", err
		}
	}
	return zipName, nil
}

func mergeMIData(csvLists []map[string]string, outDir string) error {
	finalCsvs := make(map[string][][]string)
	for i, csvList := range csvLists {
		for key, f := range csvList {
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
			for _, line := range csvLines {
				if i != 0 {
					if line[0] != "chave_coleta" && line[0] != "IdContraCheque" && line[0] != "aid" {
						finalCsvs[key] = append(finalCsvs[key], line)
					}
				} else {
					finalCsvs[key] = append(finalCsvs[key], line)
				}
			}
			// deletes the csv file after read
			if err := os.Remove(f); err != nil {
				return err
			}
		}
	}
	for key, finalCsv := range finalCsvs {
		finalCsvFile, err := os.Create(fmt.Sprintf("%s/%s.csv", outDir, key))
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
	}
	return nil
}
