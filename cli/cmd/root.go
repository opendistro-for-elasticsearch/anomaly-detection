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
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
)

const (
	defaultFileType       = "yaml"
	defaultConfigFileName = "config"
	cliName               = "esad"
)

var cfgFile string
var profile string
var user string
var password string
var endpoint string

var esadCmd = &cobra.Command{
	Use:     cliName,
	Short:   "CLI to interact with Anomaly Detection plugin in your ES cluster",
	Long:    `The ESAD Command Line Interface is a tool to manage your Anomaly Detection Plugin`,
	Version: pkg.VERSION,
}

//Execute executes commands
func Execute() {
	if err := esadCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	esadCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.esad/config.yaml)")
	esadCmd.PersistentFlags().StringVar(&user, "user", "", "user to use. Overrides config/env settings.")
	esadCmd.PersistentFlags().StringVar(&password, "password", "", "password to use. Overrides config/env settings.")
	esadCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "endpoint to use. Overrides config/env settings.")
	esadCmd.PersistentFlags().StringVar(&profile, "profile", "", "Use a specific profile from your credential file.")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		viper.AddConfigPath(filepath.Join(home, fmt.Sprintf(".%s", cliName)))
		viper.SetConfigName(defaultConfigFileName)
		viper.SetConfigType(defaultFileType)
	}

	// If a config file is found, read it in.
	viper.AutomaticEnv() // read in environment variables that match
	_ = viper.ReadInConfig()
}

//GetHandler returns handler by wiring the dependency manually
func GetHandler(c *client.Client, u *client.UserConfig) *handler.Handler {
	g := gateway.NewDetectorGateway(c, u)
	esg := es.NewESGateway(c, u)
	esc := esctrl.NewESController(esg)
	ctr := controller.NewADController(os.Stdin, esc, g)
	return handler.New(ctr)
}
