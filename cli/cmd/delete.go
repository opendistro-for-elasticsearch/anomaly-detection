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

	"github.com/spf13/cobra"
)

const commandDelete = "delete"

//deleteCmd deletes detectors based on id and name pattern.
//default input is name pattern, one can change this format to be id by passing --id flag
var deleteCmd = &cobra.Command{
	Use:   commandDelete + " [flags] [list of detectors]",
	Short: "Deletes detectors",
	Long:  `Deletes detectors based on a specified value. Use "" to make sure the name does not match with pwd lists'`,
	Run: func(cmd *cobra.Command, args []string) {
		//If no args, display usage
		if len(args) < 1 {
			displayError(cmd.Usage(), commandDelete)
			return
		}
		force, _ := cmd.Flags().GetBool("force")
		detectorID, _ := cmd.Flags().GetBool("id")
		action := handler.DeleteAnomalyDetectorByNamePattern
		if detectorID {
			action = handler.DeleteAnomalyDetectorByID
		}
		err := deleteDetectors(args, force, action)
		displayError(err, commandDelete)
	},
}

func init() {
	esadCmd.AddCommand(deleteCmd)
	deleteCmd.Flags().BoolP("force", "f", false, "Force deletion even if it is running")
	deleteCmd.Flags().BoolP("id", "", false, "Input is id")
}

func deleteDetectors(detectors []string, force bool, f func(*handler.Handler, string, bool) error) error {
	commandHandler, err := getCommandHandler()
	if err != nil {
		return err
	}
	for _, detector := range detectors {
		err = f(commandHandler, detector, force)
		if err != nil {
			return err
		}
	}
	return nil
}
