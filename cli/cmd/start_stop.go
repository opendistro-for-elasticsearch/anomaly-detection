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
	"esad/internal/handler/ad"

	"github.com/spf13/cobra"
)

const (
	commandStart = "start"
	commandStop  = "stop"
)

//startCmd stops detectors based on id and name pattern.
//default input is name pattern, one can change this format to be id by passing --id flag
var startCmd = &cobra.Command{
	Use:   commandStart + " [flags] [list of detectors]",
	Short: "Start detectors based on an id or name pattern",
	Long: fmt.Sprintf("Description:\n  " +
		`Start detectors based on a pattern. Use "" to make sure the name does not match with pwd lists'`),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			displayError(cmd.Usage(), commandStop)
			return
		}
		idStatus, _ := cmd.Flags().GetBool("id")
		action := ad.StartAnomalyDetectorByNamePattern
		if idStatus {
			action = ad.StartAnomalyDetectorByID
		}
		err := execute(action, args)
		displayError(err, commandStart)
	},
}

//stopCmd stops detectors based on id and name pattern.
//default input is name pattern, one can change this format to be id by passing --id flag
var stopCmd = &cobra.Command{
	Use:   commandStop + " [flags] [list of detectors]",
	Short: "Stop detectors based on id or name pattern",
	Long: fmt.Sprintf("Description:\n  " +
		`Stops detectors based on pattern, use "" to make sure the name is not matched with pwd lists'`),
	Run: func(cmd *cobra.Command, args []string) {
		//If no args, display usage
		if len(args) < 1 {
			displayError(cmd.Usage(), commandStop)
			return
		}
		idStatus, _ := cmd.Flags().GetBool("id")
		action := ad.StopAnomalyDetectorByNamePattern
		if idStatus {
			action = ad.StopAnomalyDetectorByID
		}
		err := execute(action, args)
		displayError(err, commandStop)
	},
}

func init() {
	esadCmd.AddCommand(startCmd)
	startCmd.Flags().BoolP("name", "", true, "input is name or pattern")
	startCmd.Flags().BoolP("id", "", false, "input is id")
	esadCmd.AddCommand(stopCmd)
	stopCmd.Flags().BoolP("name", "", true, "input is name or pattern")
	stopCmd.Flags().BoolP("id", "", false, "input is id")
}

func execute(f func(*ad.Handler, string) error, detectors []string) error {
	// iterate over the arguments
	// the first return value is index of fileNames, we can omit it using _
	commandHandler, err := getCommandHandler()
	if err != nil {
		return err
	}
	for _, detector := range detectors {
		err := f(commandHandler, detector)
		if err != nil {
			return err
		}
	}
	return nil
}

func getCommandHandler() (*ad.Handler, error) {
	newClient, err := client.New(nil)
	if err != nil {
		return nil, err
	}
	u, err := getUserProfile()
	if err != nil {
		return nil, err
	}
	return GetHandler(newClient, u), nil
}
