package utils

import "strings"

func Parser_date(unformatted_data string) *string {
	if len(unformatted_data) == 8 && unformatted_data != "00000000" {
		str := []string{unformatted_data[0:4], unformatted_data[4:6], unformatted_data[6:8]}
		ret := strings.Join(str, "-")
		return &ret
	} else {
		return nil
	}
}

func Parse_cnae(cnae_text_list string) string {
	if len(cnae_text_list) > 0 {
		cnaes := strings.Split(cnae_text_list, ",")
		return cnaes[0]
	}
	return ""
}

func Parse_ddd(ddd string) string {
	if len(ddd) > 2 {
		return ddd[len(ddd)-2 : len(ddd)-1]
	}
	return ddd
}
