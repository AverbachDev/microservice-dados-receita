package service

import (
	"fmt"
	"os/exec"
)

func Download() string {
	cmd, err := exec.Command("/bin/bash", "./download.sh").Output()

	if err != nil {
		fmt.Printf("error %s", err)
	}

	output := string(cmd)
	return output
}
