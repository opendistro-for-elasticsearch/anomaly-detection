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
	"strings"

	"github.com/spf13/cobra"
)

const (
	commandDownload   = "download"
	flagOutput        = "output"
	fileExtensionJSON = "json"
)

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
func WriteInFile(cmd *cobra.Command, d *entity.DetectorOutput) error {
	output, _ := cmd.Flags().GetString(flagOutput)
	if _, err := os.Stat(output); os.IsNotExist(err) {
		return fmt.Errorf("output directory [%s] does not exists", output)
	}
	filePath := filepath.Join(output, fmt.Sprintf("%s.%s", d.Name, fileExtensionJSON))
	if ok := isCreateFileAllowed(filePath); !ok {
		return nil
	}
	f, err := os.Create(filePath)
	defer func() {
		f.Close()
	}()
	if err != nil {
		return err
	}
	return FPrint(f, d)
}

func isCreateFileAllowed(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return true
	}
	return askForConfirmation(path)
}

func askForConfirmation(path string) bool {

	fmt.Printf("overwrite %s? (y/n [n])", filepath.Base(path))
	var response string
	_, err := fmt.Fscanln(os.Stdin, &response)
	if err != nil {
		//Exit if for some reason, we are not able to accept user input
		fmt.Printf("failed to accept value from user due to %s", err)
		return false
	}
	switch strings.ToLower(response) {
	case "y", "yes":
		return true
	case "n", "no":
		return false
	default:
		return false
	}
}

func init() {
	esadCmd.AddCommand(downloadCmd)
	downloadCmd.Flags().BoolP("name", "", true, "input is name or pattern")
	downloadCmd.Flags().BoolP("id", "", false, "input is id")
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("failed to find current working directory due to ", err)
	}
	downloadCmd.Flags().StringP(flagOutput, "o", cwd, "downloads detectors inside this folder path")
}
