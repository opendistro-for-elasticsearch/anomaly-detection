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

package es

//Terms contains fields
type Terms struct {
	Field string `json:"field"`
}

//DistinctGroups contains terms
type DistinctGroups struct {
	Term Terms `json:"terms"`
}

//Aggregate contains list of items
type Aggregate struct {
	Group DistinctGroups `json:"items"`
}

//SearchRequest structure for request
type SearchRequest struct {
	Agg  Aggregate `json:"aggs"`
	Size int32     `json:"size"`
}

//Bucket represents bucket used by ES for aggregations
type Bucket struct {
	Key      interface{} `json:"key"`
	DocCount int64       `json:"doc_count"`
}

//Items contains buckets defined by es response
type Items struct {
	Buckets []Bucket `json:"buckets"`
}

//Aggregations contains items defined by es response
type Aggregations struct {
	Items Items `json:"items"`
}

//Response response defined by es response
type Response struct {
	Aggregations Aggregations `json:"aggregations"`
}
