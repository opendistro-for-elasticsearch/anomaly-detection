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
	commandUpdate = "update"
	flagForce     = "force"
	flagStart     = "start"
)

//updateCmd updates detectors based on file configuration
var updateCmd = &cobra.Command{
	Use:   commandUpdate + " [list of file-path] [flags]",
	Short: "Updates detectors based on configurations",
	Long:  `Updates detectors based on configurations specified by file path`,
	Run: func(cmd *cobra.Command, args []string) {
		//If no args, display usage
		if len(args) < 1 {
			if err := cmd.Usage(); err != nil {
				fmt.Println(err)
			}
			return
		}
		force, _ := cmd.Flags().GetBool(flagForce)
		restart, _ := cmd.Flags().GetBool(flagStart)
		err := updateDetectors(args, force, restart)
		if err != nil {
			fmt.Println(commandUpdate, "command failed")
			fmt.Println("Reason:", err)
		}
	},
}

func init() {
	esadCmd.AddCommand(updateCmd)
	updateCmd.Flags().BoolP(flagForce, "f", false, "Will stop detector and update it")
	updateCmd.Flags().BoolP(flagStart, "r", false, "Will start detector if update is successful")
}

func updateDetectors(fileNames []string, force bool, restart bool) error {
	commandHandler, err := getCommandHandler()
	if err != nil {
		return err
	}
	for _, name := range fileNames {
		err = handler.UpdateAnomalyDetector(commandHandler, name, force, restart)
		if err != nil {
			return err
		}
	}
	return nil
}
