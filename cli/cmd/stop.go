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
	"esad/internal/handler/ad"
	"fmt"
	"github.com/spf13/cobra"
)

const commandStop = "stop"

var stopCmd = &cobra.Command{
	Use:   commandStop + " [flags] [list of detectors]",
	Short: "Stop detectors",
	Long:  `Stops detectors based on pattern, use "" to make sure the name is not matched with pwd lists'`,
	Run: func(cmd *cobra.Command, args []string) {
		idStatus, _ := cmd.Flags().GetBool("id")
		action := ad.StopAnomalyDetector
		if idStatus {
			action = ad.StopAnomalyDetectorByID
		}
		err := execute(action, args)
		if err != nil {
			fmt.Println(commandStop, "command failed")
			fmt.Println("Reason:", err)
		}
	},
}

func init() {
	esadCmd.AddCommand(stopCmd)
	stopCmd.Flags().BoolP("name", "", true, "Input is name or pattern")
	stopCmd.Flags().BoolP("id", "", false, "Input is id")
}
