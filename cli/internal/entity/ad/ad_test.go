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
	"esad/internal/mapper"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

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
        			"sum_value": {
          				"sum": {
            				"field": "value"
						}
        			}
      			}`)
}

func getCreateAnomalyDetectorData() string {
	return strings.ReplaceAll(`{
	  "name": "testdata-detector",
	  "description": "Test detector",
	  "time_field": "timestamp",
	  "indices": ["order*"],
	  "feature_attributes": [{"feature_name": "sum_value","feature_enabled": true,"aggregation_query": {"sum_value": {"sum": {"field": "value"}}}}],
	  "filter_query": {"bool": {"filter": [{"exists": {"field": "value","boost": 1}}],"adjust_pure_negative": true,"boost": 1}},
	  "detection_interval": {"period": {"interval": 1,"unit": "Minutes"}
      },
	  "window_delay": {"period": {"interval": 1,"unit": "Minutes"}}}`, "\n\t  ", "")
}
func getCreateDetector() CreateDetector {
	return CreateDetector{
		Name:        "testdata-detector",
		Description: "Testdetector",
		TimeField:   "timestamp",
		Index:       []string{"order*"},
		Features: []Feature{
			{
				Name:             "sum_value",
				Enabled:          true,
				AggregationQuery: getRawFeatureAggregation(),
			},
		},
		Filter: getRawFilter(),
		Interval: Interval{
			Period: Period{
				Duration: 1,
				Unit:     "Minutes",
			},
		},
		Delay: Interval{
			Period: Period{
				Duration: 1,
				Unit:     "Minutes",
			},
		},
	}
}

func getCreateDetectorRequest() CreateDetectorRequest {
	return CreateDetectorRequest{
		Name:        "testdata-detector",
		Description: "Test detector",
		TimeField:   "timestamp",
		Index:       []string{"order*"},
		Features: []FeatureRequest{{
			AggregationType: []string{"sum"},
			Enabled:         true,
			Field:           []string{"value"},
		}},
		Filter:         getRawFilter(),
		Interval:       "10m",
		Delay:          "5m",
		Start:          true,
		PartitionField: mapper.StringToStringPtr("ip"),
	}
}
func getCreateDetectorRequestJSON() string {
	return `{
  			"name": "testdata-detector",
  			"description": "Test detector",
  			"time_field": "timestamp",
  			"index": ["order*"],
  			"features": [{
      			"aggregation_type": ["sum"],
      			"enabled": true,
      			"field":["value"]
    		}],
  			"filter": {
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
  			},
  			"interval": "10m",
			"window_delay": "5m",
			"start": true,
            "partition_field": "ip"
		}`
}

func TestCreateDetectorRequestUnMarshalling(t *testing.T) {
	t.Run("deserialization success", func(t *testing.T) {
		test := getCreateDetectorRequestJSON()
		actual := CreateDetectorRequest{}
		_ = json.Unmarshal([]byte(test), &actual)
		expected := getCreateDetectorRequest()
		assert.EqualValues(t, expected, actual)
	})
}

func TestCreateDetectorMarshalling(t *testing.T) {
	t.Run("serialization success", func(t *testing.T) {
		test := getCreateDetector()
		actual, _ := json.Marshal(test)
		expected := getCreateAnomalyDetectorData()
		removeNewLine := strings.ReplaceAll(expected, "\n", "")
		removeTabs := strings.ReplaceAll(removeNewLine, "\n", "")
		assert.EqualValues(t, strings.ReplaceAll(removeTabs, " ", ""), string(actual))
	})
}

func TestSearchRequestMarshall(t *testing.T) {
	t.Run("serialization for search", func(t *testing.T) {
		data := getSearchRequest()
		actual, err := json.Marshal(data)
		assert.Nil(t, err)
		assert.EqualValues(t, getSearchRequestJSON(), string(actual))
	})
}

func getSearchRequestJSON() string {
	return `{"query":{"match":{"name":"test-d"}}}`
}
func getSearchRequest() interface{} {
	return SearchRequest{
		Query: SearchQuery{
			Match: Match{Name: "test-d"},
		},
	}
}

func TestGetDetectorResponse(t *testing.T) {
	t.Run("deserialization success", func(t *testing.T) {
		var actual DetectorResponse
		expected := DetectorResponse{
			ID: "m4ccEnIBTXsGi3mvMt9p",
			AnomalyDetector: AnomalyDetector{
				Metadata: Metadata{
					Name:        "test-detector",
					Description: "Test detector",
					TimeField:   "timestamp",
					Index:       []string{"order*"},
					Features: []Feature{
						{
							Name:             "total_order",
							Enabled:          true,
							AggregationQuery: []byte(`{"total_order":{"sum":{"field":"value"}}}`),
						},
					},
					Filter: []byte(`{"bool" : {"filter" : [{"exists" : {"field" : "value","boost" : 1.0}}],"adjust_pure_negative" : true,"boost" : 1.0}}`),
					Interval: Interval{
						Period: Period{
							Duration: 1,
							Unit:     "Minutes",
						},
					},
					Delay: Interval{
						Period: Period{
							Duration: 1,
							Unit:     "Minutes",
						},
					},
				},
				SchemaVersion:  0,
				LastUpdateTime: 1589441737319,
			},
		}
		responseJSON := `
		{
		  "_id" : "m4ccEnIBTXsGi3mvMt9p",
		  "_version" : 1,
		  "_primary_term" : 1,
		  "_seq_no" : 3,
		  "anomaly_detector" : {
			"name" : "test-detector",
			"description" : "Test detector",
			"time_field" : "timestamp",
			"indices" : [
			  "order*"
			],
			"filter_query" : {"bool" : {"filter" : [{"exists" : {"field" : "value","boost" : 1.0}}],"adjust_pure_negative" : true,"boost" : 1.0}},
			"detection_interval" : {
			  "period" : {
				"interval" : 1,
				"unit" : "Minutes"
			  }
			},
			"window_delay" : {
			  "period" : {
				"interval" : 1,
				"unit" : "Minutes"
			  }
			},
			"schema_version" : 0,
			"feature_attributes" : [
			  {
				"feature_id" : "mYccEnIBTXsGi3mvMd8_",
				"feature_name" : "total_order",
				"feature_enabled" : true,
				"aggregation_query" : {"total_order":{"sum":{"field":"value"}}}
			  }
			],
			"last_update_time" : 1589441737319
		  }
		}
		`
		_ = json.Unmarshal([]byte(responseJSON), &actual)
		assert.EqualValues(t, expected, actual)
	})
}
