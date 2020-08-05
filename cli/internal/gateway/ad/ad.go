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
	"esad/internal/client"
	gw "esad/internal/gateway"
	mapper2 "esad/internal/mapper"
	"fmt"
	"net/http"
	"net/url"
)

const (
	baseURL           = "_opendistro/_anomaly_detection/detectors"
	startURLTemplate  = baseURL + "/%s/" + "_start"
	stopURLTemplate   = baseURL + "/%s/" + "_stop"
	searchURLTemplate = baseURL + "/_search"
	deleteURLTemplate = baseURL + "/%s"
)

//go:generate mockgen -destination=mocks/mock_ad.go -package=mocks . Gateway

// Gateway interface to AD Plugin
type Gateway interface {
	CreateDetector(context.Context, interface{}) ([]byte, error)
	StartDetector(context.Context, string) error
	StopDetector(context.Context, string) (*string, error)
	DeleteDetector(context.Context, string) error
	SearchDetector(context.Context, interface{}) ([]byte, error)
}

type gateway struct {
	gw.HTTPGateway
}

//New creates new Gateway instance
func New(c *client.Client, u *client.UserConfig) Gateway {
	return &gateway{*gw.NewHTTPGateway(c, u)}
}

func (g *gateway) buildCreateURL() (*url.URL, error) {
	endpoint, err := gw.GetValidEndpoint(g.UserConfig)
	if err != nil {
		return nil, err
	}
	endpoint.Path = baseURL
	return endpoint, nil
}

func (g *gateway) CreateDetector(ctx context.Context, payload interface{}) ([]byte, error) {
	createURL, err := g.buildCreateURL()
	if err != nil {
		return nil, err
	}
	detectorRequest, err := g.BuildRequest(ctx, http.MethodPost, payload, createURL.String(), gw.GetHeaders())
	if err != nil {
		return nil, err
	}
	response, err := g.Call(detectorRequest, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (g *gateway) buildStartURL(ID string) (*url.URL, error) {
	endpoint, err := gw.GetValidEndpoint(g.UserConfig)
	if err != nil {
		return nil, err
	}
	endpoint.Path = fmt.Sprintf(startURLTemplate, ID)
	return endpoint, nil
}

func (g *gateway) StartDetector(ctx context.Context, ID string) error {
	startURL, err := g.buildStartURL(ID)
	if err != nil {
		return err
	}
	detectorRequest, err := g.BuildRequest(ctx, http.MethodPost, "", startURL.String(), gw.GetHeaders())
	if err != nil {
		return err
	}
	_, err = g.Call(detectorRequest, http.StatusOK)
	if err != nil {
		return err
	}
	return nil
}

func (g *gateway) buildStopURL(ID string) (*url.URL, error) {
	endpoint, err := gw.GetValidEndpoint(g.UserConfig)
	if err != nil {
		return nil, err
	}
	endpoint.Path = fmt.Sprintf(stopURLTemplate, ID)
	return endpoint, nil
}

func (g *gateway) StopDetector(ctx context.Context, ID string) (*string, error) {
	stopURL, err := g.buildStopURL(ID)
	if err != nil {
		return nil, err
	}
	detectorRequest, err := g.BuildRequest(ctx, http.MethodPost, "", stopURL.String(), gw.GetHeaders())
	if err != nil {
		return nil, err
	}
	res, err := g.Call(detectorRequest, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return mapper2.StringToStringPtr(fmt.Sprintf("%s", res)), nil
}

func (g *gateway) buildSearchURL() (*url.URL, error) {
	endpoint, err := gw.GetValidEndpoint(g.UserConfig)
	if err != nil {
		return nil, err
	}
	endpoint.Path = searchURLTemplate
	return endpoint, nil
}

func (g *gateway) SearchDetector(ctx context.Context, payload interface{}) ([]byte, error) {
	searchURL, err := g.buildSearchURL()
	if err != nil {
		return nil, err
	}
	searchRequest, err := g.BuildRequest(ctx, http.MethodPost, payload, searchURL.String(), gw.GetHeaders())
	if err != nil {
		return nil, err
	}
	response, err := g.Call(searchRequest, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (g *gateway) buildDeleteURL(ID string) (*url.URL, error) {
	endpoint, err := gw.GetValidEndpoint(g.UserConfig)
	if err != nil {
		return nil, err
	}
	endpoint.Path = fmt.Sprintf(deleteURLTemplate, ID)
	return endpoint, nil
}

func (g *gateway) DeleteDetector(ctx context.Context, ID string) error {
	deleteURL, err := g.buildDeleteURL(ID)
	if err != nil {
		return err
	}
	detectorRequest, err := g.BuildRequest(ctx, http.MethodDelete, "", deleteURL.String(), gw.GetHeaders())
	if err != nil {
		return err
	}
	_, err = g.Call(detectorRequest, http.StatusOK)
	if err != nil {
		return err
	}
	return nil
}
