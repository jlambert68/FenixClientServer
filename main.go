package main

import (
	"FenixClientServer/common_config"
	"flag"
	"fmt"
	"os"
)

func main() {
	//time.Sleep(15 * time.Second)
	FenixClientServer_main()
}

func init() {
	startUpType := flag.String("startupType", "0", "The application should be started with one of the following: LOCALHOST_NODOCKER, LOCALHOST_DOCKER, GCP")

	flag.Parse()

	switch *startUpType {
	case "LOCALHOST_NODOCKER":
		common_config.StartUpType = common_config.LocalhostNodocker

	case "LOCALHOST_DOCKER":
		common_config.StartUpType = common_config.LocalhostDocker

	case "GCP":
		common_config.StartUpType = common_config.GCP

	default:
		fmt.Println("Unknown command line flag: " + *startUpType + ". Expected one of the following: LOCALHOST_NODOCKER, LOCALHOST_DOCKER, GCP")
		os.Exit(0)

	}

	fmt.Println("common_config.StartUpType", common_config.StartUpTypeMapping[common_config.StartUpType])

}
