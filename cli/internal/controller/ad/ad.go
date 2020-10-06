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

package ad

import (
	"context"
	"encoding/json"
	"errors"
	"esad/internal/controller/es"
	entity "esad/internal/entity/ad"
	"esad/internal/gateway/ad"
	cmapper "esad/internal/mapper"
	mapper "esad/internal/mapper/ad"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cheggaaa/pb/v3"
)

//go:generate mockgen -destination=mocks/mock_ad.go -package=mocks . Controller

//Controller is an interface for the AD plugin controllers
type Controller interface {
	StartDetector(context.Context, string) error
	StopDetector(context.Context, string) error
	DeleteDetector(context.Context, string, bool, bool) error
	GetDetector(context.Context, string) (*entity.DetectorOutput, error)
	CreateAnomalyDetector(context.Context, entity.CreateDetectorRequest) (*string, error)
	CreateMultiEntityAnomalyDetector(ctx context.Context, request entity.CreateDetectorRequest, interactive bool, display bool) ([]string, error)
	SearchDetectorByName(context.Context, string) ([]entity.Detector, error)
	StartDetectorByName(context.Context, string, bool) error
	StopDetectorByName(context.Context, string, bool) error
	DeleteDetectorByName(context.Context, string, bool, bool) error
	GetDetectorsByName(context.Context, string, bool) ([]*entity.DetectorOutput, error)
	UpdateDetector(context.Context, entity.UpdateDetectorUserInput, bool, bool) error
}

type controller struct {
	reader  io.Reader
	gateway ad.Gateway
	esCtrl  es.Controller
}

//New returns new Controller instance
func New(reader io.Reader, esCtrl es.Controller, gateway ad.Gateway) Controller {
	return &controller{
		reader,
		gateway,
		esCtrl,
	}
}

func validateCreateRequest(r entity.CreateDetectorRequest) error {
	if len(r.Name) < 1 {
		return fmt.Errorf("name field cannot be empty")
	}
	if len(r.Features) < 1 {
		return fmt.Errorf("features cannot be empty")
	}
	if len(r.Index) < 1 || len(r.Index[0]) < 1 {
		return fmt.Errorf("index field cannot be empty and it should have at least one valid index")
	}
	if len(r.Interval) < 1 {
		return fmt.Errorf("interval field cannot be empty")
	}
	return nil
}

//StartDetector start detector based on DetectorID
func (c controller) StartDetector(ctx context.Context, ID string) error {
	if len(ID) < 1 {
		return fmt.Errorf("detector Id: %s cannot be empty", ID)
	}
	err := c.gateway.StartDetector(ctx, ID)
	if err != nil {
		return err
	}
	return nil
}

//StopDetector stops detector based on DetectorID
func (c controller) StopDetector(ctx context.Context, ID string) error {
	if len(ID) < 1 {
		return fmt.Errorf("detector Id: %s cannot be empty", ID)
	}
	_, err := c.gateway.StopDetector(ctx, ID)
	if err != nil {
		return err
	}
	return nil
}

func (c controller) askForConfirmation(message *string) bool {

	if message == nil {
		return true
	}
	if len(*message) > 0 {
		fmt.Print(*message)
	}

	var response string
	_, err := fmt.Fscanln(c.reader, &response)
	if err != nil {
		//Exit if for some reason, we are not able to accept user input
		fmt.Printf("failed to accept value from user due to %s", err)
		os.Exit(1)
	}
	switch strings.ToLower(response) {
	case "y", "yes":
		return true
	case "n", "no":
		return false
	default:
		fmt.Printf("I'm sorry but I didn't get what you meant, please type (y)es or (n)o and then press enter:")
		return c.askForConfirmation(cmapper.StringToStringPtr(""))
	}
}

//DeleteDetector deletes detector based on DetectorID, if force is enabled, it stops before deletes
func (c controller) DeleteDetector(ctx context.Context, id string, interactive bool, force bool) error {
	if len(id) < 1 {
		return fmt.Errorf("detector Id cannot be empty")
	}
	proceed := true
	if interactive {
		proceed = c.askForConfirmation(
			cmapper.StringToStringPtr(
				fmt.Sprintf(
					"esad will delete detector: %s . Do you want to proceed? please type (y)es or (n)o and then press enter:",
					id,
				),
			),
		)
	}
	if !proceed {
		return nil
	}
	if force {
		res, err := c.gateway.StopDetector(ctx, id)
		if err != nil {
			return err
		}
		if interactive {
			fmt.Println(*res)
		}

	}
	err := c.gateway.DeleteDetector(ctx, id)
	if err != nil {
		return err
	}
	return nil
}

//GetDetector fetch detector based on DetectorID
func (c controller) GetDetector(ctx context.Context, ID string) (*entity.DetectorOutput, error) {
	if len(ID) < 1 {
		return nil, fmt.Errorf("detector Id: %s cannot be empty", ID)
	}
	response, err := c.gateway.GetDetector(ctx, ID)
	if err != nil {
		return nil, err
	}
	var data entity.DetectorResponse
	err = json.Unmarshal(response, &data)
	if err != nil {
		return nil, err
	}
	return mapper.MapToDetectorOutput(data)
}

func processEntityError(err error) error {
	var c entity.CreateError
	data := fmt.Sprintf("%v", err)
	responseErr := json.Unmarshal([]byte(data), &c)
	if responseErr != nil {
		return err
	}
	if len(c.Error.Reason) > 0 {
		return errors.New(c.Error.Reason)
	}
	return err
}

//CreateAnomalyDetector creates detector based on user request
func (c controller) CreateAnomalyDetector(ctx context.Context, r entity.CreateDetectorRequest) (*string, error) {

	if err := validateCreateRequest(r); err != nil {
		return nil, err
	}
	payload, err := mapper.MapToCreateDetector(r)
	if err != nil {
		return nil, err
	}
	response, err := c.gateway.CreateDetector(ctx, payload)
	if err != nil {
		return nil, processEntityError(err)
	}
	var data map[string]interface{}
	_ = json.Unmarshal(response, &data)

	detectorID := fmt.Sprintf("%s", data["_id"])
	if !r.Start {
		return cmapper.StringToStringPtr(detectorID), nil
	}

	err = c.StartDetector(ctx, detectorID)
	if err != nil {
		return nil, fmt.Errorf("detector is created with id: %s, but failed to start due to %v", detectorID, err)
	}
	return cmapper.StringToStringPtr(detectorID), nil
}

func (c controller) cleanupCreatedDetectors(ctx context.Context, detectors []entity.Detector) {

	if len(detectors) < 1 {
		return
	}
	var unDeleted []entity.Detector
	for _, d := range detectors {
		err := c.DeleteDetector(ctx, d.ID, false, true)
		if err != nil {
			unDeleted = append(unDeleted, d)
		}
	}
	if len(unDeleted) > 0 {
		var names []string
		for _, d := range unDeleted {
			names = append(names, d.Name)
		}
		fmt.Println("failed to clean-up created detectors. Please clean up manually following detectors: ", strings.Join(names, ", "))
	}
}

func getFilterValues(ctx context.Context, request entity.CreateDetectorRequest, c controller) ([]interface{}, error) {
	var filterValues []interface{}
	for _, index := range request.Index {
		v, err := c.esCtrl.GetDistinctValues(ctx, index, *request.PartitionField)
		if err != nil {
			return nil, err
		}
		filterValues = append(filterValues, v...)
	}
	return filterValues, nil
}

func createProgressBar(total int) *pb.ProgressBar {
	template := `{{string . "prefix"}}{{percent . }} {{bar . "[" "=" ">" "_" "]" }} {{counters . }}{{string . "suffix"}}`
	bar := pb.New(total)
	bar.SetTemplateString(template)
	bar.SetMaxWidth(65)
	bar.Start()
	return bar
}

func buildCompoundQuery(field string, value interface{}, userFilter json.RawMessage) json.RawMessage {

	leaf1 := []byte(fmt.Sprintf(`{
    			"bool": {
      				"filter": {
          				"term": {
							"%s" : "%v"
         			 	}
        			}
				}
  			}`, field, value))
	if userFilter == nil {
		return leaf1
	}
	marshal, _ := json.Marshal(entity.Query{
		Bool: entity.Bool{
			Must: []json.RawMessage{
				leaf1, userFilter,
			},
		},
	})
	return marshal
}

//CreateMultiEntityAnomalyDetector creates multiple entity detector based on partition_by field
func (c controller) CreateMultiEntityAnomalyDetector(ctx context.Context, request entity.CreateDetectorRequest, interactive bool, display bool) ([]string, error) {
	if request.PartitionField == nil || len(*request.PartitionField) < 1 {
		result, err := c.CreateAnomalyDetector(ctx, request)
		if err != nil {
			return nil, err
		}
		return []string{*result}, err
	}
	filterValues, err := getFilterValues(ctx, request, c)
	if err != nil {
		return nil, err
	}
	if len(filterValues) < 1 {
		return nil, fmt.Errorf(
			"failed to get values for partition field: %s, check whether any data is available in index %s",
			*request.PartitionField,
			request.Index,
		)
	}
	proceed := true
	if interactive {
		proceed = c.askForConfirmation(
			cmapper.StringToStringPtr(
				fmt.Sprintf(
					"esad will create %d detector(s). Do you want to proceed? please type (y)es or (n)o and then press enter:",
					len(filterValues),
				),
			),
		)
	}
	if !proceed {
		return nil, nil
	}
	var bar *pb.ProgressBar
	if display {
		bar = createProgressBar(len(filterValues))
	}
	var detectors []string
	name := request.Name
	filter := request.Filter
	var createdDetectors []entity.Detector
	for _, value := range filterValues {
		request.Filter = buildCompoundQuery(*request.PartitionField, value, filter)
		request.Name = fmt.Sprintf("%s-%s", name, value)
		result, err := c.CreateAnomalyDetector(ctx, request)
		if err != nil {
			c.cleanupCreatedDetectors(ctx, createdDetectors)
			return nil, err
		}
		createdDetectors = append(createdDetectors, entity.Detector{
			ID:   *result,
			Name: request.Name,
		})
		detectors = append(detectors, request.Name)
		if bar != nil {
			bar.Increment()
		}
	}
	if bar != nil {
		bar.Finish()
	}
	return detectors, nil
}

//SearchDetectorByName searches detector based on name
func (c controller) SearchDetectorByName(ctx context.Context, name string) ([]entity.Detector, error) {
	if len(name) < 1 {
		return nil, fmt.Errorf("detector name cannot be empty")
	}
	payload := entity.SearchRequest{
		Query: entity.SearchQuery{
			Match: entity.Match{
				Name: name,
			},
		},
	}
	response, err := c.gateway.SearchDetector(ctx, payload)
	if err != nil {
		return nil, err
	}
	detectors, err := mapper.MapToDetectors(response, name)
	if err != nil {
		return nil, err
	}
	return detectors, nil
}

func (c controller) getDetectorsToProcess(ctx context.Context, method string, pattern string) ([]entity.Detector, error) {
	if len(pattern) < 1 {
		return nil, fmt.Errorf("name cannot be empty")
	}
	//Search Detector By Name to get ID
	matchedDetectors, err := c.SearchDetectorByName(ctx, pattern)
	if err != nil {
		return nil, err
	}
	if len(matchedDetectors) < 1 {
		fmt.Printf("no detectors matched by name %s\n", pattern)
		return nil, nil
	}
	fmt.Printf("%d detectors matched by name %s\n", len(matchedDetectors), pattern)
	for _, detector := range matchedDetectors {
		fmt.Println(detector.Name)
	}

	proceed := c.askForConfirmation(
		cmapper.StringToStringPtr(
			fmt.Sprintf("esad will %s above matched detector(s). Do you want to proceed? please type (y)es or (n)o and then press enter:", method),
		),
	)
	if !proceed {
		return nil, nil
	}
	return matchedDetectors, nil
}

func (c controller) processDetectorByAction(ctx context.Context, pattern string, action string, f func(c context.Context, s string) error, display bool) error {
	matchedDetectors, err := c.getDetectorsToProcess(ctx, action, pattern)
	if err != nil {
		return err
	}
	if matchedDetectors == nil {
		return nil
	}
	var bar *pb.ProgressBar
	if display {
		bar = createProgressBar(len(matchedDetectors))
	}
	var failedDetectors []string
	for _, detector := range matchedDetectors {
		err := f(ctx, detector.ID)
		if err != nil {
			failedDetectors = append(failedDetectors, fmt.Sprintf("%s \t Reason: %s", detector.Name, err))
			continue
		}
		if bar != nil {
			bar.Increment()
		}
	}
	if bar != nil {
		bar.Finish()
	}
	if len(failedDetectors) > 0 {
		fmt.Printf("\nfailed to %s %d following detector(s)\n", action, len(failedDetectors))
		for _, detector := range failedDetectors {
			fmt.Println(detector)
		}
	}
	return nil
}

//StartDetectorByName starts detector based on name pattern. It first calls SearchDetectorByName and then
// gets lists of detectorId and call StartDetector to start individual detectors
func (c controller) StartDetectorByName(ctx context.Context, pattern string, display bool) error {
	return c.processDetectorByAction(ctx, pattern, "start", c.StartDetector, display)
}

//StopDetectorByName stops detector based on name pattern. It first calls SearchDetectorByName and then
// gets lists of detectorId and call StopDetector to stop individual detectors
func (c controller) StopDetectorByName(ctx context.Context, pattern string, display bool) error {
	return c.processDetectorByAction(ctx, pattern, "stop", c.StopDetector, display)
}

//DeleteDetectorByName deletes detector based on name pattern. It first calls SearchDetectorByName and then
// gets lists of detectorId and call DeleteDetector to delete individual detectors
func (c controller) DeleteDetectorByName(ctx context.Context, name string, force bool, display bool) error {
	matchedDetectors, err := c.getDetectorsToProcess(ctx, "delete", name)
	if err != nil {
		return err
	}
	if matchedDetectors == nil {
		return nil
	}
	var bar *pb.ProgressBar
	if display {
		bar = createProgressBar(len(matchedDetectors))
	}
	var failedDetectors []string
	for _, detector := range matchedDetectors {
		err := c.DeleteDetector(ctx, detector.ID, false, force)
		if err != nil {
			failedDetectors = append(failedDetectors, fmt.Sprintf("%s \t Reason: %s", detector.Name, err))
			continue
		}
		if bar != nil {
			bar.Increment()
		}
	}
	if bar != nil {
		bar.Finish()
	}
	if len(failedDetectors) > 0 {
		fmt.Printf("failed to delete %d following detector(s)\n", len(failedDetectors))
		for _, detector := range failedDetectors {
			fmt.Println(detector)
		}
	}
	return nil
}

//GetDetectorsByName get detector based on name pattern. It first calls SearchDetectorByName and then
// gets lists of detectorId and call GetDetector to get individual detector configuration
func (c controller) GetDetectorsByName(ctx context.Context, pattern string, display bool) ([]*entity.DetectorOutput, error) {
	matchedDetectors, err := c.getDetectorsToProcess(ctx, "fetch", pattern)
	if err != nil {
		return nil, err
	}
	if matchedDetectors == nil {
		return nil, nil
	}
	var bar *pb.ProgressBar
	if display {
		bar = createProgressBar(len(matchedDetectors))
	}
	var output []*entity.DetectorOutput
	for _, detector := range matchedDetectors {
		data, err := c.GetDetector(ctx, detector.ID)
		if err != nil {
			return nil, err
		}
		output = append(output, data)
		if bar != nil {
			bar.Increment()
		}
	}
	if bar != nil {
		bar.Finish()
	}
	return output, nil
}

//UpdateDetector updates detector based on DetectorID, if force is enabled, it overrides without checking whether
// user downloaded latest version before updating it, if start is true, detector will be started after update
func (c controller) UpdateDetector(ctx context.Context, input entity.UpdateDetectorUserInput, force bool, start bool) error {
	if len(input.ID) < 1 {
		return fmt.Errorf("detector Id cannot be empty")
	}
	if !force {
		latestDetector, err := c.GetDetector(ctx, input.ID)
		if err != nil {
			return err
		}
		if latestDetector.LastUpdatedAt > input.LastUpdatedAt {
			return fmt.Errorf(
				"new version for detector is available. Please fetch latest version and then merge your changes")
		}
	}
	proceed := c.askForConfirmation(
		cmapper.StringToStringPtr(
			fmt.Sprintf(
				"esad will update detector: %s . Do you want to proceed? please type (y)es or (n)o and then press enter:",
				input.ID,
			),
		),
	)
	if !proceed {
		return nil
	}
	if force { // stop detector implicit since force is true
		err := c.StopDetector(ctx, input.ID)
		if err != nil {
			return err
		}
	}
	payload, err := mapper.MapToUpdateDetector(input)
	if err != nil {
		return err
	}
	err = c.gateway.UpdateDetector(ctx, input.ID, payload)
	if err != nil {
		return err
	}
	if !start {
		return nil
	}
	return c.StartDetector(ctx, input.ID) // Start Detector if successfully updated it
}
