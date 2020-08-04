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
	"bytes"
	"context"
	"encoding/json"
	"esad/internal/client"
	"esad/internal/client/mocks"
	"esad/internal/entity/ad"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"
)

func helperLoadBytes(t *testing.T, name string) []byte {
	path := filepath.Join("testdata", name) // relative path
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func getTestClient(t *testing.T, response string, code int, method string, action string) *client.Client {
	testClient := mocks.NewTestClient(func(req *http.Request) *http.Response {
		// Test request parameters
		assert.Equal(t, req.URL.String(), "http://localhost:9200/_opendistro/_anomaly_detection/detectors/id"+action)
		assert.EqualValues(t, req.Method, method)
		assert.EqualValues(t, len(req.Header), 2)
		return &http.Response{
			StatusCode: code,
			// Send response to be tested
			Body: ioutil.NopCloser(bytes.NewBufferString(response)),
			// Must be set to non-nil value or it panics
			Header: make(http.Header),
		}
	})
	return testClient
}

func TestGateway_StartDetector(t *testing.T) {
	ctx := context.Background()
	t.Run("connection failed", func(t *testing.T) {
		testClient := getTestClient(t, `connection failed`, 400, http.MethodPost, "/_start")

		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		err := testGateway.StartDetector(ctx, "id")
		assert.EqualError(t, err, "connection failed")
	})
	t.Run("started successfully", func(t *testing.T) {
		testClient := getTestClient(t, `{
		  "_id" : "id",
		  "_version" : 1,
		  "_seq_no" : 6,
		  "_primary_term" : 1
		}`, 200, http.MethodPost, "/_start")
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		err := testGateway.StartDetector(ctx, "id")
		assert.NoError(t, err)
	})
}
func TestGateway_StopDetector(t *testing.T) {
	ctx := context.Background()
	t.Run("connection failed", func(t *testing.T) {
		testClient := getTestClient(t, `connection failed`, 400, http.MethodPost, "/_stop")

		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		_, err := testGateway.StopDetector(ctx, "id")
		assert.EqualError(t, err, "connection failed")
	})
	t.Run("stop successfully", func(t *testing.T) {
		testClient := getTestClient(t, `Stopped detector: id`, 200, http.MethodPost, "/_stop")
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		res, err := testGateway.StopDetector(ctx, "id")
		assert.NoError(t, err)
		assert.EqualValues(t, *res, "Stopped detector: id")
	})
}

func TestGateway_DeleteDetector(t *testing.T) {
	ctx := context.Background()
	t.Run("connection failed", func(t *testing.T) {
		testClient := getTestClient(t, `connection failed`, 400, http.MethodDelete, "")
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		err := testGateway.DeleteDetector(ctx, "id")
		assert.EqualError(t, err, "connection failed")
	})
	t.Run("delete success", func(t *testing.T) {
		testClient := getTestClient(t, `
		{
		  "_index" : ".opendistro-anomaly-detectors",
		  "_type" : "_doc",
		  "_id" : "id",
		  "_version" : 2,
		  "result" : "deleted",
		  "forced_refresh" : true,
		  "_shards" : {
			"total" : 2,
			"successful" : 2,
			"failed" : 0
		  },
		  "_seq_no" : 6,
		  "_primary_term" : 1
		}`, 200, http.MethodDelete, "")
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		err := testGateway.DeleteDetector(ctx, "id")
		assert.NoError(t, err)
	})
}

func TestGateway_SearchDetector(t *testing.T) {
	responseData, _ := json.Marshal(helperLoadBytes(t, "search_result.json"))
	ctx := context.Background()
	t.Run("search succeeded", func(t *testing.T) {

		testClient := getSearchClient(t, responseData, 200)
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		response, err := testGateway.SearchDetector(ctx, ad.SearchRequest{
			Query: ad.SearchQuery{
				Match: ad.Match{
					Name: "detector-name",
				},
			}})
		assert.NoError(t, err)
		assert.EqualValues(t, response, responseData)
	})
	t.Run("search failed due to 404", func(t *testing.T) {

		testClient := getSearchClient(t, []byte("No connection found"), 400)
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		_, err := testGateway.SearchDetector(ctx, ad.SearchRequest{
			Query: ad.SearchQuery{
				Match: ad.Match{
					Name: "detector-name",
				},
			}})
		assert.EqualError(t, err, "No connection found")
	})
}

func TestGateway_CreateDetector(t *testing.T) {
	responseData, _ := json.Marshal(helperLoadBytes(t, "create_result.json"))
	ctx := context.Background()
	t.Run("create succeeded", func(t *testing.T) {

		testClient := getCreateClient(t, responseData, 201)
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		response, err := testGateway.CreateDetector(ctx, getCreateDetector())
		assert.NoError(t, err)
		assert.EqualValues(t, response, responseData)
	})

	t.Run("create failed due to 400", func(t *testing.T) {

		testClient := getCreateClient(t, []byte("No connection found"), 400)
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		_, err := testGateway.CreateDetector(ctx, getCreateDetector())
		assert.EqualError(t, err, "No connection found")
	})
}

func getSearchClient(t *testing.T, responseData []byte, code int) *client.Client {
	testClient := mocks.NewTestClient(func(req *http.Request) *http.Response {
		// Test request parameters
		assert.Equal(t, req.URL.String(), "http://localhost:9200/_opendistro/_anomaly_detection/detectors/_search")
		assert.EqualValues(t, req.Method, http.MethodPost)
		resBytes, _ := ioutil.ReadAll(req.Body)
		var body ad.SearchRequest
		err := json.Unmarshal(resBytes, &body)
		assert.NoError(t, err)
		assert.EqualValues(t, body.Query.Match.Name, "detector-name")
		assert.EqualValues(t, len(req.Header), 2)
		return &http.Response{
			StatusCode: code,
			// Send response to be tested
			Body: ioutil.NopCloser(bytes.NewBufferString(string(responseData))),
			// Must be set to non-nil value or it panics
			Header: make(http.Header),
		}
	})
	return testClient
}

func getRawFeatureAggregation() []byte {
	return []byte(`{"sum_value":{"sum":{"field":"value"}}}`)
}

func getCreateDetector() ad.CreateDetector {
	return ad.CreateDetector{
		Name:        "testdata-detector",
		Description: "Test detector",
		TimeField:   "timestamp",
		Index:       []string{"order*"},
		Features: []ad.Feature{
			{
				Name:             "sum_value",
				Enabled:          true,
				AggregationQuery: getRawFeatureAggregation(),
			},
		},
		Filter: []byte("{}"),
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

func getCreateClient(t *testing.T, responseData []byte, code int) *client.Client {
	return mocks.NewTestClient(func(req *http.Request) *http.Response {
		// Test request parameters
		assert.Equal(t, req.URL.String(), "http://localhost:9200/_opendistro/_anomaly_detection/detectors")
		assert.EqualValues(t, req.Method, http.MethodPost)
		resBytes, _ := ioutil.ReadAll(req.Body)
		var body ad.CreateDetector
		err := json.Unmarshal(resBytes, &body)
		assert.NoError(t, err)
		assert.Equal(t, getCreateDetector(), body)
		assert.EqualValues(t, 2, len(req.Header))
		return &http.Response{
			StatusCode: code,
			// Send response to be tested
			Body: ioutil.NopCloser(bytes.NewBufferString(string(responseData))),
			// Must be set to non-nil value or it panics
			Header: make(http.Header),
		}
	})
}

func TestGateway_GetDetector(t *testing.T) {
	ctx := context.Background()
	t.Run("connection failed", func(t *testing.T) {
		testClient := getTestClient(t, `connection failed`, 400, http.MethodGet, "")
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		_, err := testGateway.GetDetector(ctx, "id")
		assert.EqualError(t, err, "connection failed")
	})
	t.Run("get success", func(t *testing.T) {
		testClient := getTestClient(t, string(helperLoadBytes(t, "get_result.json")), 200, http.MethodGet, "")
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		resp, err := testGateway.GetDetector(ctx, "id")
		assert.NoError(t, err)
		assert.EqualValues(t, helperLoadBytes(t, "get_result.json"), resp)
	})
}
