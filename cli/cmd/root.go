/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package cmd

import (
	"esad/internal/client"
	controller "esad/internal/controller/ad"
	esctrl "esad/internal/controller/es"
	gateway "esad/internal/gateway/ad"
	"esad/internal/gateway/es"
	handler "esad/internal/handler/ad"
	"esad/pkg"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	cliName               = "esad"
	defaultConfigFileName = "config"
	defaultFileType       = "yaml"
	esadConfigFile        = "ESAD_CONFIG_FILE"
	FlagConfig            = "config"
	FlagEndpoint          = "endpoint"
	FlagPassword          = "password"
	FlagProfile           = "profile"
	FlagUser              = "user"
)

var cfgFile string
var profile string
var user string
var password string
var endpoint string

var esadCmd = &cobra.Command{
	Use:     cliName,
	Short:   "Use the CLI to interact with the Anomaly Detection plugin in your ES cluster",
	Long:    `The ESAD CLI is a tool to manage your Anomaly Detection plugin`,
	Version: pkg.VERSION,
}

// Execute executes the root command.
func Execute() error {
	return esadCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	esadCmd.PersistentFlags().StringVar(&cfgFile, FlagConfig, "", "config file (default is $HOME/.esad/config.yaml)")
	esadCmd.PersistentFlags().StringVar(&user, FlagUser, "", "user to use. Overrides config/env settings.")
	esadCmd.PersistentFlags().StringVar(&password, FlagPassword, "", "password to use. Overrides config/env settings.")
	esadCmd.PersistentFlags().StringVar(&endpoint, FlagEndpoint, "", "endpoint to use. Overrides config/env settings.")
	esadCmd.PersistentFlags().StringVar(&profile, FlagProfile, "", "Use a specific profile from your credential file.")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else if value, ok := os.LookupEnv(esadConfigFile); ok {
		viper.SetConfigFile(value)
	} else {
		createDefaultConfigFile()
	}
	// If a config file is found, read it in.
	viper.AutomaticEnv() // read in environment variables that match
	_ = viper.ReadInConfig()
}

func createDefaultConfigFile() {
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	folderPath := filepath.Join(home, fmt.Sprintf(".%s", cliName))
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		if err = os.Mkdir(folderPath, 0755); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	viper.AddConfigPath(folderPath)
	viper.SetConfigName(defaultConfigFileName)
	viper.SetConfigType(defaultFileType)
}

//GetHandler returns handler by wiring the dependency manually
func GetHandler(c *client.Client, u *client.UserConfig) *handler.Handler {
	g := gateway.New(c, u)
	esg := es.New(c, u)
	esc := esctrl.New(esg)
	ctr := controller.New(os.Stdin, esc, g)
	return handler.New(ctr)
}
