package entity

type SocioEmpresa struct {
	IdEmpresa                            string  `json:"idEmpresa,omitempty"`
	TipoPessoa                           int32   `json:"tipoPessoa,omitempty"`
	Nome                                 string  `json:"nome,omitempty"`
	CpfCnpj                              string  `json:"cpfCnpj,omitempty"`
	CodigoQualificacao                   string  `json:"codigoQualificacao,omitempty"`
	Data                                 *string `json:"data,omitempty"`
	CpfRepresentanteLegal                string  `json:"cpfRepresentanteLegal,omitempty"`
	NomeRepresentanteLegal               string  `json:"nomeRepresentanteLegal,omitempty"`
	CodigoQualificacaoRepresentanteLegal string  `json:"codigoQualificacaoRepresentanteLegal,omitempty"`
	Id                                   int32   `json:"id"`
	Codigo                               string  `json:"codigo,omitempty"`
	Descricao                            string  `json:"descricao,omitempty"`
}
