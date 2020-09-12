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
	entity "esad/internal/entity/ad"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const commandDownload = "download"

//downloadCmd downloads detectors configuration on current working directory
//based on detector id or name patter
var downloadCmd = &cobra.Command{
	Use:   commandDownload + " [flags] [list of detectors]",
	Short: "Downloads detectors configurations based on id or name pattern",
	Long: fmt.Sprintf("Description:\n  " +
		`Downloads detectors configurations based on id or name pattern, use "" to make sure the name is not matched with pwd lists'`),
	Run: func(cmd *cobra.Command, args []string) {
		//If no args, display usage
		if len(args) < 1 {
			displayError(cmd.Usage(), commandDownload)
			return
		}
		err := printDetectors(WriteInFile, cmd, args)
		displayError(err, commandDownload)
	},
}

//WriteInFile writes detector's configuration on file
//file will be created inside current working directory,
//with detector name as file name
func WriteInFile(d *entity.DetectorOutput) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	filePath := filepath.Join(cwd, d.Name)
	f, err := os.Create(filePath)
	defer func() {
		f.Close()
	}()
	if err != nil {
		return err
	}
	return FPrint(f, d)
}

func init() {
	esadCmd.AddCommand(downloadCmd)
	downloadCmd.Flags().BoolP("name", "", true, "input is name or pattern")
	downloadCmd.Flags().BoolP("id", "", false, "input is id")
}
