package entity

type Empresa struct {
	Id                      string `json:"id"`
	RazaoSocial             string `json:"razaoSocial,omitempty"`
	CodigoNaturezaJuridica  string `json:"codigoNaturezaJuridica,omitempty"`
	QualificacaoResponsavel string `json:"qualificacaoResponsavel,omitempty"`
	CapitalSocial           string `json:"capitalSocial,omitempty"`
	Porte                   int32  `json:"porte,omitempty"`
}
