package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/dadosjusbr/datapackage"
	"github.com/dadosjusbr/storage"
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
		// Itera sobre todas as agências: para o caso do parâmetro agency/year ser vazio.
		for _, ag := range agencies {
			amiMap, err := client.Db.GetMonthlyInfo([]storage.Agency{{ID: ag.ID}}, year)
			if err != nil {
				log.Fatalf("error while agreggating by agency/year -- error fetching data: %v", err)
			}
			amis, ok := amiMap[ag.ID]
			if !ok {
				log.Fatalf("error while agreggating by agency/year -- there is no ami for %s", ag.ID)
			}
			pkgPath, err := createAggregatedPackage(year, outDir, ag.ID, amis)
			if err != nil {
				log.Fatalf("error while agreggating by agency/year: %q", err)
			}
			if err := updateDB(pkgPath, year); err != nil {
				log.Fatalf("error while agreggating by agency/year: %q", err)
			}
		}
	default:
		log.Fatalf("please, select some grouping to aggregate")
	}
	fmt.Printf("dados agregados!")
}

func createAggregatedPackage(year int, outDir, agency string, amis []storage.AgencyMonthlyInfo) (string, error) {
	packages, err := getBackupData(amis)
	if err != nil {
		return "", fmt.Errorf("error getting backup data (%s, %d):%w", agency, year, err)
	}
	pkgs, err := downloadPackages(packages, year, agency, outDir)
	if err != nil {
		return "", fmt.Errorf("error downloading datapackage (%s, %d):%w", agency, year, err)
	}
	var rc datapackage.ResultadoColeta_CSV
	for _, pkg := range pkgs {
		aux, err := datapackage.Load(pkg)
		if err != nil {
			return "", fmt.Errorf("error loading datapackage (%s):%w", pkg, err)
		}
		rc.Coleta = append(rc.Coleta, aux.Coleta...)
		rc.Folha = append(rc.Folha, aux.Folha...)
		rc.Metadados = append(rc.Metadados, aux.Metadados...)
		rc.Remuneracoes = append(rc.Remuneracoes, aux.Remuneracoes...)
	}
	pkgName := filepath.Join(outDir, fmt.Sprintf("%s-%d.zip", agency, year))
	if err := datapackage.Zip(pkgName, rc, true); err != nil {
		return "", fmt.Errorf("error creating datapackage (%s):%w", pkgName, err)
	}
	return pkgName, nil
}

func updateDB(dataPackageFilename string, year int) error {
	fmt.Println("arquivo final criado:", dataPackageFilename)
	var nogrop *string // necessária pois não queremos agrupamento por grupo nesse momento.
	packBackup, err := client.Cloud.UploadFile(dataPackageFilename, *nogrop)
	if err != nil {
		return err
	}
	if err := client.StorePackage(storage.Package{
		AgencyID: nil,
		Year:     &year,
		Month:    nil,
		Group:    nil,
		Package:  *packBackup}); err != nil {
		return err
	}
	fmt.Println("arquivo de backup criado", packBackup)
	return nil
}

func downloadPackages(packages []extractionData, year int, agency string, outDir string) ([]string, error) {
	var pkgs []string
	for _, p := range packages {
		if filepath.Ext(p.URL) == ".zip" {
			zFName := fmt.Sprintf("%d_%d_%s.zip", year, p.Month, agency)
			zPath := filepath.Join(outDir, zFName)
			fmt.Println("arquivo baixado:", zPath)
			if err := download(zPath, p.URL); err != nil {
				return nil, err
			}
			pkgs = append(pkgs, zPath)
		}
	}
	return pkgs, nil
}

func getBackupData(amis []storage.AgencyMonthlyInfo) ([]extractionData, error) {
	var pkgs []extractionData
	for _, ami := range amis {
		if ami.Package != nil {
			pkgs = append(pkgs,
				extractionData{Year: ami.Year,
					Month: ami.Month,
					URL:   ami.Package.URL,
					Hash:  ami.Package.Hash})
		}
	}
	return pkgs, nil
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
