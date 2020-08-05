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
	"esad/internal/entity/ad"
	"esad/internal/mapper"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func helperLoadBytes(t *testing.T, name string) []byte {
	path := filepath.Join("testdata", name) // relative path
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func getRawFilter() []byte {
	return []byte(`{
		"bool": {
			"filter": [{
				"exists": {
				"field": "value",
				"boost": 1
				}
			}],
			"adjust_pure_negative": true,
			"boost": 1
		}
}`)
}

func getRawFeatureAggregation() []byte {
	return []byte(`{
        			"sum_order": {
          				"sum": {
            				"field": "order"
						}
        			}
      			}`)
}

func getCreateDetector() ad.CreateDetector {
	return ad.CreateDetector{
		Name:        "testdata-detector",
		Description: "Test detector",
		TimeField:   "timestamp",
		Index:       []string{"order*"},
		Features: []ad.Feature{
			{
				Name:             "sum_order",
				Enabled:          true,
				AggregationQuery: getRawFeatureAggregation(),
			},
		},
		Filter: getRawFilter(),
		Interval: ad.Interval{
			Period: ad.Period{
				Duration: 1,
				Unit:     "Minutes",
			},
		},
		Delay: ad.Interval{
			Period: ad.Period{
				Duration: 1,
				Unit:     "Minutes",
			},
		},
	}
}

func getCreateDetectorRequest(interval string, delay string) ad.CreateDetectorRequest {
	return ad.CreateDetectorRequest{
		Name:        "testdata-detector",
		Description: "Test detector",
		TimeField:   "timestamp",
		Index:       []string{"order*"},
		Features: []ad.FeatureRequest{{
			AggregationType: []string{"sum"},
			Enabled:         true,
			Field:           []string{"order"},
		}},
		Filter:         getRawFilter(),
		Interval:       interval,
		Delay:          delay,
		Start:          true,
		PartitionField: mapper.StringToStringPtr("ip"),
	}
}
func TestMapToCreateDetector(t *testing.T) {
	t.Run("Success: Valid Input", func(t *testing.T) {
		r := getCreateDetectorRequest("1m", "1m")
		actual, err := MapToCreateDetector(r)
		expected := getCreateDetector()
		assert.NoError(t, err)
		assert.EqualValues(t, expected, *actual)
	})
	t.Run("Failure: interval val", func(t *testing.T) {
		r := getCreateDetectorRequest("m1", "1m")
		_, err := MapToCreateDetector(r)
		assert.Error(t, err)
	})
	t.Run("Failure: interval unit", func(t *testing.T) {
		r := getCreateDetectorRequest("1", "1m")
		_, err := MapToCreateDetector(r)
		assert.Error(t, err)
	})
	t.Run("Failure: interval wrong unit", func(t *testing.T) {
		r := getCreateDetectorRequest("1y", "1m")
		_, err := MapToCreateDetector(r)
		assert.Error(t, err)
	})
	t.Run("Failure: window delay val", func(t *testing.T) {
		r := getCreateDetectorRequest("1m", "m1")
		_, err := MapToCreateDetector(r)
		assert.Error(t, err)
	})
	t.Run("Failure: window delay unit", func(t *testing.T) {
		r := getCreateDetectorRequest("1m", "1")
		_, err := MapToCreateDetector(r)
		assert.Error(t, err)
	})
	t.Run("Failure: window delay wrong unit", func(t *testing.T) {
		r := getCreateDetectorRequest("1m", "1y")
		_, err := MapToCreateDetector(r)
		assert.Error(t, err)
	})
}

func TestMapToDetectors(t *testing.T) {
	t.Run("filter detectors", func(t *testing.T) {
		actual, err := MapToDetectors(helperLoadBytes(t, "search_response.json"), "test-detector-ecommerce0-T*")
		expected := []ad.Detector{
			{
				Name: "test-detector-ecommerce0-Tuesday",
				ID:   "6lh0bnMBLlLTlH7nz4iE",
			},
			{
				Name: "test-detector-ecommerce0-Thursday",
				ID:   "ylh0bnMBLlLTlH7nzohq",
			},
		}
		assert.Nil(t, err)
		assert.ElementsMatch(t, expected, actual)
	})
	t.Run("filter detectors for any ", func(t *testing.T) {
		actual, err := MapToDetectors(helperLoadBytes(t, "search_response.json"), "test-detector-ecommerce0-T*")
		expected := []ad.Detector{
			{
				Name: "test-detector-ecommerce0-Tuesday",
				ID:   "6lh0bnMBLlLTlH7nz4iE",
			},
			{
				Name: "test-detector-ecommerce0-Thursday",
				ID:   "ylh0bnMBLlLTlH7nzohq",
			},
		}
		assert.Nil(t, err)
		assert.ElementsMatch(t, expected, actual)
	})
	t.Run("filter detectors for at least", func(t *testing.T) {
		actual, err := MapToDetectors(helperLoadBytes(t, "search_response.json"), "test-detector-ecommerce0-Tuesday+")
		assert.Nil(t, err)
		assert.ElementsMatch(t, []ad.Detector{}, actual)
	})
	t.Run("filter detectors for exact", func(t *testing.T) {
		actual, err := MapToDetectors(helperLoadBytes(t, "search_response.json"), "test-detector-ecommerce0-Tuesday")
		expected := []ad.Detector{
			{
				Name: "test-detector-ecommerce0-Tuesday",
				ID:   "6lh0bnMBLlLTlH7nz4iE",
			},
		}
		assert.Nil(t, err)
		assert.ElementsMatch(t, expected, actual)
	})
	t.Run("filter detectors for no match", func(t *testing.T) {
		actual, err := MapToDetectors(helperLoadBytes(t, "search_response.json"), "test-detector-ecommerce0-Tuesda")
		assert.Nil(t, err)
		assert.ElementsMatch(t, []ad.Detector{}, actual)
	})
}
