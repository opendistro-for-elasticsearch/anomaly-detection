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
	controller "esad/internal/controller/ad"
	entity "esad/internal/entity/ad"
	mapper2 "esad/internal/mapper"
	"fmt"
	"io/ioutil"
	"os"
)

//Handler is facade for controller
type Handler struct {
	controller.Controller
}

// New returns new Handler instance
func New(controller controller.Controller) *Handler {
	return &Handler{
		controller,
	}
}

//CreateAnomalyDetector creates detector based on file configurations
func CreateAnomalyDetector(h *Handler, fileName string, interactive bool) error {
	return h.CreateAnomalyDetector(fileName, interactive)
}

//GenerateAnomalyDetector generate sample detector to provide skeleton for users
func GenerateAnomalyDetector() ([]byte, error) {
	return json.MarshalIndent(entity.CreateDetectorRequest{
		Name:        "Detector Name",
		Description: "A brief description",
		TimeField:   "",
		Index:       []string{},
		Features: []entity.FeatureRequest{
			{
				AggregationType: []string{"count"},
				Enabled:         false,
				Field:           []string{},
			},
		},
		Filter:         []byte("{}"),
		Interval:       "10m",
		Delay:          "1m",
		Start:          false,
		PartitionField: mapper2.StringToStringPtr(""),
	}, "", "  ")
}

//CreateAnomalyDetector creates detector based on file configurations
func (h *Handler) CreateAnomalyDetector(fileName string, interactive bool) error {
	if len(fileName) < 1 {
		return fmt.Errorf("file name cannot be empty")
	}

	jsonFile, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s due to %v", fileName, err)
	}
	defer func() {
		err := jsonFile.Close()
		if err != nil {
			fmt.Println("failed close json file due to ", err)
		}
	}()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var request entity.CreateDetectorRequest
	err = json.Unmarshal(byteValue, &request)
	if err != nil {
		return fmt.Errorf("file %s cannot be accepted due to %v", fileName, err)
	}
	ctx := context.Background()
	names, err := h.CreateMultiEntityAnomalyDetector(ctx, request, interactive, true)
	if err != nil {
		return err
	}
	if len(names) > 0 {
		fmt.Printf("Successfully created %d detector(s)", len(names))
		fmt.Println()
		return nil
	}
	return err
}

//DeleteAnomalyDetectorByID deletes detector based on detectorId
func DeleteAnomalyDetectorByID(h *Handler, detectorID string, force bool) error {
	return h.DeleteAnomalyDetectorByID(detectorID, force)
}

//DeleteAnomalyDetectorByID deletes detector based on detectorId
func (h *Handler) DeleteAnomalyDetectorByID(detectorID string, force bool) error {

	ctx := context.Background()
	err := h.DeleteDetector(ctx, detectorID, true, force)
	if err != nil {
		return err
	}
	return err
}

//DeleteAnomalyDetectorByNamePattern deletes detector based on detectorName
func DeleteAnomalyDetectorByNamePattern(h *Handler, detectorName string, force bool) error {
	return h.DeleteAnomalyDetectorByNamePattern(detectorName, force)
}

//DeleteAnomalyDetectorByNamePattern deletes detector based on detectorName
func (h *Handler) DeleteAnomalyDetectorByNamePattern(detectorName string, force bool) error {

	ctx := context.Background()
	err := h.DeleteDetectorByName(ctx, detectorName, force, true)
	if err != nil {
		return err
	}
	return err
}

//StartAnomalyDetectorByID starts detector based on detector id
func StartAnomalyDetectorByID(h *Handler, detector string) error {
	return h.StartAnomalyDetectorByID(detector)
}

//StartAnomalyDetectorByID starts detector based on detector id
func (h *Handler) StartAnomalyDetectorByID(detector string) error {

	ctx := context.Background()
	err := h.StartDetector(ctx, detector)
	if err != nil {
		return err
	}
	return nil
}

// StartAnomalyDetectorByNamePattern starts detector based on detector name pattern
func StartAnomalyDetectorByNamePattern(h *Handler, detector string) error {
	return h.StartAnomalyDetectorByNamePattern(detector)
}

// StartAnomalyDetectorByNamePattern starts detector based on detector name pattern
func (h *Handler) StartAnomalyDetectorByNamePattern(detector string) error {

	ctx := context.Background()
	err := h.StartDetectorByName(ctx, detector, true)
	if err != nil {
		return err
	}
	return nil
}

// StopAnomalyDetectorByNamePattern stops detector based on detector name pattern
func StopAnomalyDetectorByNamePattern(h *Handler, detector string) error {
	return h.StopAnomalyDetectorByNamePattern(detector)
}

// StopAnomalyDetectorByNamePattern stops detector based on detector name pattern
func (h *Handler) StopAnomalyDetectorByNamePattern(detector string) error {

	ctx := context.Background()
	err := h.StopDetectorByName(ctx, detector, true)
	if err != nil {
		return err
	}
	return nil
}

// StopAnomalyDetectorByID stops detector based on detector id
func StopAnomalyDetectorByID(h *Handler, detector string) error {
	return h.StopAnomalyDetectorByID(detector)
}

// StopAnomalyDetectorByID stops detector based on detector id
func (h *Handler) StopAnomalyDetectorByID(detector string) error {

	ctx := context.Background()
	err := h.StopDetector(ctx, detector)
	if err != nil {
		return err
	}
	return nil
}
