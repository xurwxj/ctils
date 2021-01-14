package main

import (
	"bytes"
	"fmt"

	"github.com/xurwxj/ctils/oss/aws"
	"github.com/xurwxj/viper"
)

func main() {
	testChunkUpload()
}

func testChunkUpload() {
	initConfig()
	bn, endoint, err := aws.ChunkUpload("cnhz", "test/jkjkjsl", "pub", "../../../git.shining3d.com/cloud/algorithm/tmp/2021-01-08_003_111_谭彩红.zip")
	// bn, endoint, err := ali.ChunkUpload("cnhz", "test/jkjkjsl", "pub", "../../../git.shining3d.com/cloud/algorithm/tmp/2021-01-08_003_111_谭彩红.zip")
	fmt.Println("bn: ", bn)
	fmt.Println("endoint: ", endoint)
	fmt.Println("err: ", err)

}

func initConfig() {
	viper.AddConfigPath(".")
	viper.SetConfigName(".server")

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
		fmt.Println("ReadInConfig keys: ", viper.AllKeys())
	} else {
		fmt.Println("viper ReadInConfig set err: ", err)
		logConfig := `
		{
			"log": {
				"output": "file",
				"path": "logs",
				"file": "service.log",
				"level": "info",
				"max": 10,
				"maxAge": 30,
				"localtime": true
			},
			"version": ""
		}
		`
		if err := viper.ReadConfig(bytes.NewReader([]byte(logConfig))); err == nil {
			fmt.Println("ReadConfig keys: ", viper.AllKeys())
		} else {
			fmt.Println("viper ReadConfig set err: ", err)
		}
	}
}
