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
	"esad/internal/client"
	elasticsearch "esad/internal/entity/es"
	gw "esad/internal/gateway"
	"fmt"
	"net/http"
	"net/url"
)

const search = "_search"

//go:generate mockgen -destination=mocks/mock_es.go -package=mocks . Gateway

//Gateway interface to call ES
type Gateway interface {
	SearchDistinctValues(ctx context.Context, index string, field string) ([]byte, error)
}

type gateway struct {
	gw.HTTPGateway
}

// New returns new Gateway instance
func New(c *client.Client, u *client.UserConfig) Gateway {
	return &gateway{
		*gw.NewHTTPGateway(c, u),
	}
}
func buildPayload(field string) *elasticsearch.SearchRequest {
	return &elasticsearch.SearchRequest{
		Size: 0, // This will skip data in the response
		Agg: elasticsearch.Aggregate{
			Group: elasticsearch.DistinctGroups{
				Term: elasticsearch.Terms{
					Field: field,
				},
			},
		},
	}
}

func (g *gateway) buildSearchURL(index string) (*url.URL, error) {
	endpoint, err := gw.GetValidEndpoint(g.UserConfig)
	if err != nil {
		return nil, err
	}
	endpoint.Path = fmt.Sprintf("%s/%s", index, search)
	return endpoint, nil
}

//SearchDistinctValues gets distinct values on index for given field
func (g *gateway) SearchDistinctValues(ctx context.Context, index string, field string) ([]byte, error) {
	searchURL, err := g.buildSearchURL(index)
	if err != nil {
		return nil, err
	}
	searchRequest, err := g.BuildRequest(ctx, http.MethodGet, buildPayload(field), searchURL.String(), gw.GetHeaders())
	if err != nil {
		return nil, err
	}
	response, err := g.Call(searchRequest, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return response, nil
}
