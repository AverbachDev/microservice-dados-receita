package entity

type CNPJEmpresa struct {
	RazaoSocial             string  `json:"razaoSocial,omitempty"`
	IdEmpresa               string  `json:"idEmpresa,omitempty"`
	Subsidiaria             string  `json:"subsidiaria,omitempty"`
	CodigoVerificador       string  `json:"codigoVerificador,omitempty"`
	Cnpj                    string  `json:"cnpj,omitempty"`
	MatrizFilial            bool    `json:"matrizFilial,omitempty"`
	Fantasia                string  `json:"fantasia,omitempty"`
	SituacaoCadastral       string  `json:"situacaoCadastral,omitempty"`
	DataSituacaoCadastral   *string `json:"dataSituacaoCadastral,omitempty"`
	MotivoSituacaoCadastral string  `json:"motivoSituacaoCadastral,omitempty"`
	DataAbertura            *string `json:"dataAbertura,omitempty"`
	CnaePrincipal           string  `json:"cnaePrincipal,omitempty"`
	CnaeSecundaria          string  `json:"cnaeSecundaria,omitempty"`
	EnderecoTipoLogradouro  string  `json:"enderecoTipoLogradouro,omitempty"`
	EnderecoLogradouro      string  `json:"enderecoLogradouro,omitempty"`
	EnderecoNumero          string  `json:"enderecoNumero,omitempty"`
	EnderecoComplemento     string  `json:"enderecoComplemento,omitempty"`
	EnderecoBairro          string  `json:"enderecoBairro,omitempty"`
	EnderecoCep             string  `json:"enderecoCep,omitempty"`
	EnderecoUf              string  `json:"enderecoUf,omitempty"`
	EnderecoCodigoMunicipio string  `json:"enderecoCodigoMunicipio,omitempty"`
	Telefone1Ddd            string  `json:"telefone1Ddd,omitempty"`
	Telefone1Numero         string  `json:"telefone1Numero,omitempty"`
	Telefone2Ddd            string  `json:"telefone2Ddd,omitempty"`
	Telefone2Numero         string  `json:"telefone2Numero,omitempty"`
	FaxDdd                  string  `json:"faxDdd,omitempty"`
	FaxNumero               string  `json:"faxNumero,omitempty"`
	Email                   string  `json:"email,omitempty"`
	Id                      string  `json:"id,omitempty"`
	NomeMunicipio           string  `json:"nomeMunicipio,omitempty"`
}
