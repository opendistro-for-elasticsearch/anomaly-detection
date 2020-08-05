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
	"context"
	"encoding/json"
	"esad/internal/entity/es"
	"esad/internal/gateway/es"
	"fmt"
)

//go:generate mockgen -destination=mocks/mock_es.go -package=mocks . Controller

//Controller is an interface for ES Cluster to get distinct values
type Controller interface {
	GetDistinctValues(ctx context.Context, index string, field string) ([]interface{}, error)
}

type controller struct {
	gateway es.Gateway
}

//New returns new instance of Controller
func New(gateway es.Gateway) Controller {
	return &controller{
		gateway,
	}
}
func (c controller) GetDistinctValues(ctx context.Context, index string, field string) ([]interface{}, error) {
	if len(index) == 0 || len(field) == 0 {
		return nil, fmt.Errorf("index and field cannot be empty")
	}
	response, err := c.gateway.SearchDistinctValues(ctx, index, field)
	if err != nil {
		return nil, err
	}
	var data elasticsearch.Response
	err = json.Unmarshal(response, &data)
	if err != nil {
		return nil, err
	}

	var values []interface{}
	for _, bucket := range data.Aggregations.Items.Buckets {
		values = append(values, bucket.Key)
	}
	return values, nil
}
