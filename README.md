# DadosJusBr - Agregador

O Agregador é usado para organizar e consolidar os arquivos de remunerações mensais do DadosJusBr para download. Ele é estruturado como um script em GO que utiliza de flags para selecionar dados mensais baseado em um agrupamento específico, e consolida todos os arquivos CSV da seleção agrupada em um único.

Para escolher um grupo é necessário passar a flag `group_by` e dizer qual o agrupamento é o desejado, agregações podem ser agrupadas das seguintes maneiras:

- Por um Órgão em um determinado ano: `--group_by=agency/year --agency=mppb`

- Todos os Órgãos individualmente em um determinado ano: `--group_by=agency/year`

- Por uma UF ou Grupo Jurídico (MP, TJ) em um determinado ano: `group_by=group/year --group=AL`

## Como usar

- É preciso ter o compilador de Go instalado em sua máquina. Mais informações [aqui](https://golang.org/dl/).
- Um arquivo .env.example na pasta raíz indica as variáveis de ambiente que precisam ser passadas para o agregador.

### As flags de possível uso no agregador são:

- `grop_by`: O grupo da agreação
- `year`: O ano da agregação
- `outDir` (opitional): O diretorio de saída onde os dados agregados serão mantidos
- `agency` (opitional): O órgão selecionado para agregar
- `group` (opitional): A UF ou Grupo Jurídico (MP, TJ) selecionado para agregar

### Os comandos para gerar um build e executar o agregador são:

```sh
go get
go build -o agregador
./agregador --group_by=${GRUPO_DE_AGREGAÇÃO} --year=${ANO} --agency=${ORGAO}
```