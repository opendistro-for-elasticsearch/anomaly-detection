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
	"encoding/json"
	entity "esad/internal/entity/ad"
	"esad/internal/handler/ad"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

const (
	commandCat = "cat"
)

//catCmd prints detectors configuration based on id and name pattern.
var catCmd = &cobra.Command{
	Use:   commandCat + " [flags] [list of detectors]",
	Short: "Concatenate and print detectors based on id or name pattern",
	Long:  `concatenate and print detectors based on pattern, use "" to make sure the name is not matched with pwd lists'`,
	Run: func(cmd *cobra.Command, args []string) {
		//If no args, display usage
		if len(args) < 1 {
			if err := cmd.Usage(); err != nil {
				fmt.Println(err)
			}
			return
		}
		printDetectors(Println, cmd, args)
	},
}

//printDetectors print detectors
func printDetectors(f func(*entity.DetectorOutput), cmd *cobra.Command, detectors []string) {
	idStatus, _ := cmd.Flags().GetBool("id")
	commandHandler, err := getCommandHandler()
	if err != nil {
		fmt.Println(err)
	}
	// default is name
	action := ad.GetAnomalyDetectorsByNamePattern
	if idStatus {
		action = getDetectorsByID
	}
	results, err := getDetectors(commandHandler, detectors, action)
	if err != nil {
		fmt.Println(err)
		return
	}
	print(f, results)
}

func getDetectors(
	commandHandler *ad.Handler, args []string, get func(*ad.Handler, string) (
		[]*entity.DetectorOutput, error)) ([]*entity.DetectorOutput, error) {
	var results []*entity.DetectorOutput
	for _, detector := range args {
		output, err := get(commandHandler, detector)
		if err != nil {
			return nil, err
		}
		results = append(results, output...)
	}
	return results, nil
}

//getDetectorsByID gets detector output based on ID as argument
func getDetectorsByID(commandHandler *ad.Handler, ID string) ([]*entity.DetectorOutput, error) {

	output, err := ad.GetAnomalyDetectorByID(commandHandler, ID)
	if err != nil {
		return nil, err
	}
	return []*entity.DetectorOutput{output}, nil
}

//print displays the list of output.
func print(f func(*entity.DetectorOutput), results []*entity.DetectorOutput) {
	if results == nil {
		return
	}
	for _, d := range results {
		f(d)
	}
}

//FPrint prints detector configuration on writer
//Since this is json format, use indent function to pretty print before printing on writer
func FPrint(writer io.Writer, d *entity.DetectorOutput) error {
	formattedOutput, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(writer, string(formattedOutput))
	return err
}

//Println prints detector configuration on stdout
func Println(d *entity.DetectorOutput) {
	err := FPrint(os.Stdout, d)
	if err != nil {
		fmt.Println(err)
	}
}

func init() {
	esadCmd.AddCommand(catCmd)
	catCmd.Flags().BoolP("name", "", true, "Input is name or pattern")
	catCmd.Flags().BoolP("id", "", false, "Input is id")
}
