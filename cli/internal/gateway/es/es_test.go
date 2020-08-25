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

import (
	"bytes"
	"context"
	"encoding/json"
	"esad/internal/client"
	"esad/internal/client/mocks"
	elasticsearch "esad/internal/entity/es"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func helperLoadBytes(t *testing.T, name string) []byte {
	path := filepath.Join("testdata", name) // relative path
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func getTestClient(t *testing.T, responseData string, code int) *client.Client {
	return mocks.NewTestClient(func(req *http.Request) *http.Response {
		// Test request parameters
		assert.Equal(t, req.URL.String(), "http://localhost:9200/test_index/_search")
		resBytes, _ := ioutil.ReadAll(req.Body)
		var body elasticsearch.SearchRequest
		err := json.Unmarshal(resBytes, &body)
		assert.NoError(t, err)
		assert.EqualValues(t, body.Size, 0)
		assert.EqualValues(t, body.Agg.Group.Term.Field, "day_of_week")
		assert.EqualValues(t, len(req.Header), 2)
		return &http.Response{
			StatusCode: code,
			// Send response to be tested
			Body: ioutil.NopCloser(bytes.NewBufferString(responseData)),
			// Must be set to non-nil value or it panics
			Header: make(http.Header),
		}
	})
}

func TestGateway_SearchDistinctValues(t *testing.T) {
	responseData, _ := json.Marshal(helperLoadBytes(t, "search_result.json"))
	ctx := context.Background()
	t.Run("search succeeded", func(t *testing.T) {

		testClient := getTestClient(t, string(responseData), 200)
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		actual, err := testGateway.SearchDistinctValues(ctx, "test_index", "day_of_week")
		assert.NoError(t, err)
		assert.EqualValues(t, actual, responseData)
	})
	t.Run("search failed due to 404", func(t *testing.T) {
		testClient := getTestClient(t, "No connection found", 400)
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "admin",
			Password: "admin",
		})
		_, err := testGateway.SearchDistinctValues(ctx, "test_index", "day_of_week")
		assert.EqualError(t, err, "No connection found")
	})
	t.Run("search failed due to bad user config", func(t *testing.T) {

		testClient := getTestClient(t, "No connection found", 400)
		testGateway := New(testClient, &client.UserConfig{
			Endpoint: "http://localhost:9200",
			Username: "",
			Password: "admin",
		})
		_, err := testGateway.SearchDistinctValues(ctx, "test_index", "day_of_week")
		assert.EqualError(t, err, "user name and password cannot be empty")
	})
}
