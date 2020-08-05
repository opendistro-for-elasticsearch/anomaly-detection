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

package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"esad/internal/client"
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"io/ioutil"
	"net/url"
)

//HTTPGateway type for gateway client
type HTTPGateway struct {
	Client     *client.Client
	UserConfig *client.UserConfig
}

//GetHeaders returns common headers
func GetHeaders() map[string]string {
	return map[string]string{
		"Content-Type": "application/json",
	}
}

//NewHTTPGateway creats new HTTPGateway instance
func NewHTTPGateway(c *client.Client, u *client.UserConfig) *HTTPGateway {
	return &HTTPGateway{
		Client:     c,
		UserConfig: u,
	}
}

//Call calls request using http
func (g *HTTPGateway) Call(req *retryablehttp.Request, statusCode int) ([]byte, error) {

	res, err := g.Client.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := res.Body.Close()
		if err != nil {
			return
		}
	}()
	resBytes, _ := ioutil.ReadAll(res.Body)
	if res.StatusCode != statusCode {
		return nil, fmt.Errorf("%s", string(resBytes))
	}
	return resBytes, nil

}

//BuildRequest builds request based on method and appends payload for given url with headers
func (g *HTTPGateway) BuildRequest(ctx context.Context, method string, payload interface{}, url string, headers map[string]string) (*retryablehttp.Request, error) {
	reqBytes, _ := json.Marshal(payload)
	reqReader := bytes.NewReader(reqBytes)
	r, err := retryablehttp.NewRequest(method, url, reqReader)
	if err != nil {
		return nil, err
	}
	req := r.WithContext(ctx)
	if len(g.UserConfig.Username) == 0 || len(g.UserConfig.Password) == 0 {
		return nil, fmt.Errorf("user name and password cannot be empty")
	}
	req.SetBasicAuth(g.UserConfig.Username, g.UserConfig.Password)
	if len(headers) == 0 {
		return req, nil
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return req, nil
}

//GetValidEndpoint get url based on user config
func GetValidEndpoint(userConfig *client.UserConfig) (*url.URL, error) {
	if len(userConfig.Endpoint) == 0 {
		return &url.URL{
			Scheme: "https",
			Host:   "localhost:9200",
		}, nil
	}
	u, err := url.Parse(userConfig.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint: %v due to %v", userConfig.Endpoint, err)
	}
	return u, nil
}
