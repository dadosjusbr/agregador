package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/dadosjusbr/datapackage"
	"github.com/dadosjusbr/storage"
	"github.com/dadosjusbr/storage/models"
	"github.com/dadosjusbr/storage/repo/database"
	"github.com/dadosjusbr/storage/repo/file_storage"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	PostgresUser     string `envconfig:"POSTGRES_USER" required:"true"`
	PostgresPassword string `envconfig:"POSTGRES_PASSWORD" required:"true"`
	PostgresDBName   string `envconfig:"POSTGRES_DBNAME" required:"true"`
	PostgresHost     string `envconfig:"POSTGRES_HOST" required:"true"`
	PostgresPort     string `envconfig:"POSTGRES_PORT" required:"true"`

	AWSRegion    string `envconfig:"AWS_REGION" required:"true"`
	S3Bucket     string `envconfig:"S3_BUCKET" required:"true"`
	AWSAccessKey string `envconfig:"AWS_ACCESS_KEY_ID" required:"true"`
	AWSSecretKey string `envconfig:"AWS_SECRET_ACCESS_KEY" required:"true"`

	Agency       string `envconfig:"AID" required:"true"`
	Year         int    `envconfig:"YEAR" required:"true"`
	OutputFolder string `envconfig:"OUTPUT_FOLDER" required:"true"`
}

type extractionData struct {
	Year  int
	Month int
	URL   string
	Hash  string
	Size  int64
}

func main() {
	var conf config
	if err := envconfig.Process("", &conf); err != nil {
		log.Fatal(err)
	}
	//Criando o client do Postgres
	postgresDb, err := database.NewPostgresDB(conf.PostgresUser, conf.PostgresPassword, conf.PostgresDBName, conf.PostgresHost, conf.PostgresPort)
	if err != nil {
		log.Fatalf("error creating Postgres client: %v", err.Error())
	}
	// Criando o client do S3
	s3Client, err := file_storage.NewS3Client(conf.AWSRegion, conf.S3Bucket)
	if err != nil {
		log.Fatalf("error creating S3 client: %v", err.Error())
	}
	// Criando o client do storage a partir do banco postgres e do client do s3
	pgS3Client, err := storage.NewClient(postgresDb, s3Client)
	if err != nil {
		log.Fatalf("error setting up postgres storage client: %s", err)
	}
	defer pgS3Client.Db.Disconnect()

	// Agregando os dados por ano
	amiMap, err := pgS3Client.Db.GetMonthlyInfo([]models.Agency{{ID: conf.Agency}}, conf.Year)
	if err != nil {
		log.Fatalf("error while agreggating by agency/year -- error fetching data: %v", err)
	}

	amis, ok := amiMap[conf.Agency]
	if !ok {
		log.Fatalf("error while agreggating by agency/year -- there is no ami for %s", conf.Agency)
	}
	pkgPath, err := createAggregatedPackage(conf.Year, conf.OutputFolder, conf.Agency, amis)
	if err != nil {
		log.Fatalf("error while agreggating by agency/year: %q", err)
	}

	pkgS3Key := fmt.Sprintf("%s/datapackage/%s", conf.Agency, filepath.Base(pkgPath))

	_, err = pgS3Client.Cloud.UploadFile(pkgPath, pkgS3Key)
	if err != nil {
		log.Fatalf("Error while uploading package/year: %q", err)
	}

	// Agregando os dados por órgão
	annualSummaryMap, err := pgS3Client.GetAnnualSummary(conf.Agency)
	if err != nil {
		log.Fatalf("error while getting annual summary -- error fetching data: %v", err)
	}

	pkgAgencyPath, err := createAggregatedPackageByAgency(conf.Agency, conf.OutputFolder, annualSummaryMap)
	if err != nil {
		log.Fatalf("error while agreggating by agency -- error fetching data: %v", err)
	}

	pkgAgencyS3Key := fmt.Sprintf("%s/datapackage/%s", conf.Agency, filepath.Base(pkgAgencyPath))

	_, err = pgS3Client.Cloud.UploadFile(pkgAgencyPath, pkgAgencyS3Key)
	if err != nil {
		log.Fatalf("Error while uploading package/agency: %q", err)
	}
}

func createAggregatedPackage(year int, outDir, agency string, amis []models.AgencyMonthlyInfo) (string, error) {
	packages, err := getBackupData(amis)
	if err != nil {
		return "", fmt.Errorf("error getting backup data (%s, %d):%w", agency, year, err)
	}
	pkgs, err := downloadPackages(packages, year, agency, outDir)
	if err != nil {
		return "", fmt.Errorf("error downloading datapackage (%s, %d):%w", agency, year, err)
	}
	pkgName := filepath.Join(outDir, fmt.Sprintf("%s-%d.zip", agency, year))
	if err := createRc(pkgs, pkgName, agency, outDir); err != nil {
		return "", fmt.Errorf("error zipping agency data (%s): %w", pkgName, err)
	}
	return pkgName, nil
}

func createAggregatedPackageByAgency(agency string, outDir string, as []models.AnnualSummary) (string, error) {
	var pkgs []string
	for _, a := range as {
		zPkg := filepath.Join(outDir, fmt.Sprintf("%s-%d.zip", agency, a.Year))
		err := download(zPkg, a.Package.URL)
		if err != nil {
			return "", fmt.Errorf("error dowloading packages/year (%s): %w", zPkg, err)
		}
		pkgs = append(pkgs, zPkg)
	}

	pkgName := filepath.Join(outDir, fmt.Sprintf("%s.zip", agency))
	if err := createRc(pkgs, pkgName, agency, outDir); err != nil {
		return "", fmt.Errorf("error zipping agency data (%s): %w", pkgName, err)
	}
	return pkgName, nil
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

func getBackupData(amis []models.AgencyMonthlyInfo) ([]extractionData, error) {
	var pkgs []extractionData
	for _, ami := range amis {
		if ami.Package != nil {
			pkgs = append(pkgs,
				extractionData{Year: ami.Year,
					Month: ami.Month,
					URL:   ami.Package.URL,
					Hash:  ami.Package.Hash,
					Size:  ami.Package.Size})
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

func createRc(pkgs []string, zPkgName, agency, outDir string) error {
	var rc datapackage.ResultadoColeta_CSV_V2
	for _, pkg := range pkgs {
		aux, err := datapackage.LoadV2(pkg)
		if err != nil {
			return fmt.Errorf("error loading datapackage (%s):%w", pkg, err)
		}
		rc.Coleta = append(rc.Coleta, aux.Coleta...)
		rc.Folha = append(rc.Folha, aux.Folha...)
		rc.Metadados = append(rc.Metadados, aux.Metadados...)
		rc.Remuneracoes = append(rc.Remuneracoes, aux.Remuneracoes...)
	}
	if err := datapackage.ZipV2(zPkgName, rc, true); err != nil {
		return fmt.Errorf("error creating datapackage (%s):%w", zPkgName, err)
	}
	return nil
}
