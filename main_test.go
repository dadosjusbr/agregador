package main

import (
	"testing"

	"github.com/dadosjusbr/storage"
	"github.com/stretchr/testify/assert"
)

var (
	agencies = []storage.Agency{
		{
			ID:      "mppb",
			Name:    "Ministério Public da Paraíba",
			Type:    "MP",
			Entity:  "M",
			UF:      "PB",
			FlagURL: ""}}
	year   = 2021
	outDir = "test"
)

func TestAgreggationByAgencyYear(t *testing.T) {
	t.Run("should go to database get monthly info from 2021 in mppb and agregate", func(t *testing.T) {
		err := agregateDataByAgencyYear(year, outDir, agencies)
		assert.Truef(t, err != nil, "Erro agregando dados")
	})
}
