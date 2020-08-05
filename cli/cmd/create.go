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
	handler "esad/internal/handler/ad"
	"fmt"
	"github.com/spf13/cobra"
)

const (
	commandCreate = "create"
	interactive   = "interactive"
	generate      = "generate-skeleton"
)

//createCmd creates detectors based on file configuration, if interactive mode is on,
//this command will prompt for confirmation on number of detectors will be created on executions.
var createCmd = &cobra.Command{
	Use:   commandCreate + " [list of file-path] [flags]",
	Short: "Creates detectors based on configurations",
	Long:  `Creates detectors based on configurations specified by file path`,
	Run: func(cmd *cobra.Command, args []string) {
		generate, _ := cmd.Flags().GetBool(generate)
		if generate {
			generateTemplate()
			return
		}
		//If no args, display usage
		if len(args) < 1 {
			if err := cmd.Usage(); err != nil {
				fmt.Println(err)
			}
			return
		}
		status, _ := cmd.Flags().GetBool(interactive)
		err := createDetectors(args, status)
		if err != nil {
			fmt.Println(commandCreate, "command failed")
			fmt.Println("Reason:", err)
		}
	},
}

func generateTemplate() {
	detector, _ := handler.GenerateAnomalyDetector()
	fmt.Println(string(detector))
}

func init() {
	esadCmd.AddCommand(createCmd)
	createCmd.Flags().BoolP(interactive, "i", false, "Create Detectors in an interactive way")
	createCmd.Flags().BoolP(generate, "g", false, "Outputs Detector's configuration")

}

func createDetectors(fileNames []string, status bool) error {
	commandHandler, err := getCommandHandler()
	if err != nil {
		return err
	}
	for _, name := range fileNames {
		err = handler.CreateAnomalyDetector(commandHandler, name, status)
		if err != nil {
			return err
		}
	}
	return nil
}
