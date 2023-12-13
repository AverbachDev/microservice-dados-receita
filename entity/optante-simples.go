package entity

type OptanteSimples struct {
	IdEmpresa     string  `json:"idEmpresa"`
	Simples       string  `json:"simples,omitempty"`
	SimplesInicio *string `json:"simplesInicio,omitempty"`
	SimplesFim    *string `json:"simplesFim,omitempty"`
	Simei         string  `json:"simei,omitempty"`
	SimeiInicio   *string `json:"simeiInicio,omitempty"`
	SimeiFim      *string `json:"simeiFim,omitempty"`
}
