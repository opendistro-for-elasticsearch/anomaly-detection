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
	"encoding/json"
	"esad/internal/entity/ad"
	"esad/internal/mapper"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	featureCountLimit = 5
	minutesKey        = "m"
	minutes           = "Minutes"
)

func getFeatureAggregationQuery(name string, agg string, field string) ([]byte, error) {

	userTypeToESType := make(map[string]string)
	userTypeToESType["average"] = "avg"
	userTypeToESType["count"] = "value_count"
	userTypeToESType["sum"] = "sum"
	userTypeToESType["min"] = "min"
	userTypeToESType["max"] = "max"
	val, ok := userTypeToESType[strings.ToLower(agg)]
	if !ok {
		var allowedTypes []string
		for key := range userTypeToESType {
			allowedTypes = append(allowedTypes, key)
		}
		return nil, fmt.Errorf("invalid aggeration type: '%s', only allowed types are: %s ", agg, strings.Join(allowedTypes, ","))
	}
	agg = val
	return []byte(fmt.Sprintf(`{
        			"%s": {
          				"%s": {
            				"field": "%s"
						}
        			}
      			}`, name, agg, field)), nil
}
func mapToFeature(r ad.FeatureRequest) ([]ad.Feature, error) {
	var features []ad.Feature
	for _, t := range r.AggregationType {
		for _, f := range r.Field {
			name := fmt.Sprintf("%s_%s", t, f)
			query, err := getFeatureAggregationQuery(name, t, f)
			if err != nil {
				return nil, err
			}
			features = append(features, ad.Feature{
				Name:             name,
				Enabled:          r.Enabled,
				AggregationQuery: query,
			})
		}
	}
	return features, nil
}

func getUnit(request string) (*string, error) {

	//extract last character
	unit := strings.ToLower(request[len(request)-1:])
	if unit != minutesKey {
		return nil, fmt.Errorf("invalid unit: '%v' in %v, only %s (%s) is supported", unit, request, minutesKey, minutes)
	}
	return mapper.StringToStringPtr(minutes), nil
}

func getUnitKey(request string) (*string, error) {

	if request != minutes {
		return nil, fmt.Errorf("invalid request: '%v', only %s is supported", request, minutes)
	}
	return mapper.StringToStringPtr(minutesKey), nil
}

func getDuration(request string) (*int32, error) {
	//extract last but one character
	duration, err := strconv.Atoi(request[:len(request)-1])
	if err != nil {
		return nil, fmt.Errorf("invalid duration: %v, due to {%v}", request, err)
	}
	if duration < 0 {
		return nil, fmt.Errorf("duration must be positive integer")
	}
	return mapper.IntToInt32Ptr(duration)
}

func mapToInterval(request string) (*ad.Interval, error) {
	if len(request) < 2 {
		return nil, fmt.Errorf("invalid format: %s", request)
	}
	duration, err := getDuration(request)
	if err != nil {
		return nil, err
	}
	unit, err := getUnit(request)
	if err != nil {
		return nil, err
	}
	return &ad.Interval{
		Period: ad.Period{
			Duration: mapper.Int32PtrToInt32(duration),
			Unit:     mapper.StringPtrToString(unit),
		},
	}, nil
}

func mapIntervalToStringPtr(request ad.Interval) (*string, error) {
	duration := request.Period.Duration
	unit, err := getUnitKey(request.Period.Unit)
	if err != nil {
		return nil, err
	}
	return mapper.StringToStringPtr(fmt.Sprintf("%d%s", duration, *unit)), nil
}

//MapToCreateDetector maps to CreateDetector
func MapToCreateDetector(request ad.CreateDetectorRequest) (*ad.CreateDetector, error) {

	var features []ad.Feature
	err := validateFeatureLimit(request.Features)
	if err != nil {
		return nil, err
	}
	for _, f := range request.Features {
		ftr, err := mapToFeature(f)
		if err != nil {
			return nil, err
		}
		features = append(features, ftr...)
	}

	interval, err := mapToInterval(request.Interval)
	if err != nil {
		return nil, err
	}
	delay, err := mapToInterval(request.Delay)
	if err != nil {
		return nil, err
	}
	return &ad.CreateDetector{
		Name:        request.Name,
		Description: request.Description,
		TimeField:   request.TimeField,
		Index:       request.Index,
		Features:    features,
		Filter:      request.Filter,
		Interval:    *interval,
		Delay:       *delay,
	}, nil
}

func validateFeatureLimit(features []ad.FeatureRequest) error {
	featureCount := 0
	for _, f := range features {
		featureCount += len(f.AggregationType) * len(f.Field)
	}
	if featureCount == 0 || featureCount > featureCountLimit {
		return fmt.Errorf("trying to create %d feautres, only upto %d features are allowed", featureCount, featureCountLimit)
	}
	return nil
}

//MapToDetectors maps response to detectors
func MapToDetectors(searchResponse []byte, name string) ([]ad.Detector, error) {
	var data ad.SearchResponse
	err := json.Unmarshal(searchResponse, &data)
	if err != nil {
		return nil, err
	}
	var result []ad.Detector
	processedNameAnyCharacter := strings.ReplaceAll(name, "*", "(.*)")
	processedName := strings.ReplaceAll(processedNameAnyCharacter, "+", "(.+)")

	r, _ := regexp.Compile(fmt.Sprintf("^%s$", processedName))
	for _, detector := range data.Hits.Hits {
		if !r.MatchString(detector.Source.Name) {
			continue
		}
		result = append(result, ad.Detector{
			Name: detector.Source.Name,
			ID:   detector.ID,
		})
	}
	return result, nil
}

func MapToDetectorOutput(response ad.DetectorResponse) (*ad.DetectorOutput, error) {
	delay, err := mapIntervalToStringPtr(response.AnomalyDetector.Delay)
	if err != nil {
		return nil, err
	}
	interval, err := mapIntervalToStringPtr(response.AnomalyDetector.Interval)
	if err != nil {
		return nil, err
	}
	return &ad.DetectorOutput{
		ID:            response.ID,
		Name:          response.AnomalyDetector.Name,
		Description:   response.AnomalyDetector.Description,
		TimeField:     response.AnomalyDetector.TimeField,
		Index:         response.AnomalyDetector.Index,
		Features:      response.AnomalyDetector.Features,
		Filter:        response.AnomalyDetector.Filter,
		Interval:      mapper.StringPtrToString(interval),
		Delay:         mapper.StringPtrToString(delay),
		LastUpdatedAt: response.AnomalyDetector.LastUpdateTime,
		SchemaVersion: response.AnomalyDetector.SchemaVersion,
	}, nil
}
