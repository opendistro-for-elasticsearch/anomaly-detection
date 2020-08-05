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
)

//Feature structure for detector features
type Feature struct {
	Name             string          `json:"feature_name"`
	Enabled          bool            `json:"feature_enabled"`
	AggregationQuery json.RawMessage `json:"aggregation_query"`
}

//Period represents time interval
type Period struct {
	Duration int32  `json:"interval"`
	Unit     string `json:"unit"`
}

//Interval represent unit of time
type Interval struct {
	Period Period `json:"period"`
}

//CreateDetector represents Detector creation request
type CreateDetector struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	TimeField   string          `json:"time_field"`
	Index       []string        `json:"indices"`
	Features    []Feature       `json:"feature_attributes"`
	Filter      json.RawMessage `json:"filter_query,omitempty"`
	Interval    Interval        `json:"detection_interval"`
	Delay       Interval        `json:"window_delay"`
}

//FeatureRequest represents feature request
type FeatureRequest struct {
	AggregationType []string `json:"aggregation_type"`
	Enabled         bool     `json:"enabled"`
	Field           []string `json:"field"`
}

//CreateDetectorRequest represents request for AD
type CreateDetectorRequest struct {
	Name           string           `json:"name"`
	Description    string           `json:"description"`
	TimeField      string           `json:"time_field"`
	Index          []string         `json:"index"`
	Features       []FeatureRequest `json:"features"`
	Filter         json.RawMessage  `json:"filter,omitempty"`
	Interval       string           `json:"interval"`
	Delay          string           `json:"window_delay"`
	Start          bool             `json:"start"`
	PartitionField *string          `json:"partition_field"`
}

//Bool type for must query
type Bool struct {
	Must []json.RawMessage `json:"must"`
}

//Query type to represent query
type Query struct {
	Bool Bool `json:"bool"`
}

//Detector type to map name to ID
type Detector struct {
	Name string
	ID   string
}

//CreateFailedError structure if create failed
type CreateFailedError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

//CreateError Error type in Create Response
type CreateError struct {
	Error  CreateFailedError `json:"error"`
	Status int32             `json:"status"`
}

//Profile represents profile in config
type Profile struct {
	Endpoint string `mapstructure:"endpoint"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Name     string `mapstructure:"name"`
}

//Configuration represents configuration in config file
type Configuration struct {
	Profiles []Profile `mapstructure:"profiles"`
}

//Match specifies name
type Match struct {
	Name string `json:"name"`
}

//SearchQuery contains match names
type SearchQuery struct {
	Match Match `json:"match"`
}

//SearchRequest represents structure for search detectors
type SearchRequest struct {
	Query SearchQuery `json:"query"`
}

//Source contains detectors metadata
type Source struct {
	Name string `json:"name"`
}

//Hit contains search results
type Hit struct {
	ID     string `json:"_id"`
	Source Source `json:"_source"`
}

//Container represents structure for search response
type Container struct {
	Hits []Hit `json:"hits"`
}

//SearchResponse represents structure for search response
type SearchResponse struct {
	Hits Container `json:"hits"`
}

type Metadata CreateDetector

type AnomalyDetector struct {
	Metadata
	SchemaVersion  int32  `json:"schema_version"`
	LastUpdateTime uint64 `json:"last_update_time"`
}

//DetectorResponse represents detector's setting
type DetectorResponse struct {
	ID              string          `json:"_id"`
	AnomalyDetector AnomalyDetector `json:"anomaly_detector"`
}

//DetectorOutput represents detector's setting displayed to user
type DetectorOutput struct {
	ID            string
	Name          string          `json:"name"`
	Description   string          `json:"description"`
	TimeField     string          `json:"time_field"`
	Index         []string        `json:"indices"`
	Features      []Feature       `json:"features"`
	Filter        json.RawMessage `json:"filter_query"`
	Interval      string          `json:"detection_interval"`
	Delay         string          `json:"window_delay"`
	LastUpdatedAt uint64          `json:"last_update_time"`
	SchemaVersion int32           `json:"schema_version"`
}
