// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquerydataprofile

import (
	"context"
	"fmt"
	"time"

	dataplexapi "cloud.google.com/go/dataplex/apiv1"
	dataplexpb "cloud.google.com/go/dataplex/apiv1/dataplexpb"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	bigqueryds "github.com/googleapis/genai-toolbox/internal/sources/bigquery"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/googleapis/genai-toolbox/internal/util/parameters"
	"google.golang.org/api/iterator"
)

const kind string = "bigquery-list-data-scans"

func init() {
	if !tools.Register(kind, newConfig) {
		panic(fmt.Sprintf("tool kind %q already registered", kind))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type compatibleSource interface {
	MakeDataplexDataScanClient() func() (*dataplexapi.DataScanClient, bigqueryds.DataplexClientCreator, error)
	BigQueryProject() string
	UseClientAuthorization() bool
}

type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source" validate:"required"`
	Description  string   `yaml:"description"`
	AuthRequired []string `yaml:"authRequired"`
}

// validate interface
var _ tools.ToolConfig = Config{}

func (cfg Config) ToolConfigKind() string {
	return kind
}

func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
	location := parameters.NewStringParameter("location", "This refers to a Google Cloud region.")
	project := parameters.NewStringParameterWithDefault("project", "", "The Google Cloud project ID. If not provided, the tool defaults to the project from the source configuration.")
	state := parameters.NewStringParameterWithDefault("state", "", "State of the datascan, if not provided tool will return datascan with any state.")
	pageSize := parameters.NewIntParameterWithDefault("pageSize", 5, "Number of results in the search page.")

	params := parameters.Parameters{location, project, state, pageSize}

	description := "Use this tool to get a list of data scans of a project."
	if cfg.Description != "" {
		description = cfg.Description
	}
	mcpManifest := tools.GetMcpManifest(cfg.Name, description, cfg.AuthRequired, params, nil)

	t := Tool{
		Config:     cfg,
		Parameters: params,
		manifest: tools.Manifest{
			Description:  cfg.Description,
			Parameters:   params.Manifest(),
			AuthRequired: cfg.AuthRequired,
		},
		mcpManifest: mcpManifest,
	}
	return t, nil
}

type Tool struct {
	Config
	Parameters  parameters.Parameters
	manifest    tools.Manifest
	mcpManifest tools.McpManifest
}

func (t Tool) ToConfig() tools.ToolConfig {
	return t.Config
}

func (t Tool) Authorized(verifiedAuthServices []string) bool {
	return tools.IsAuthorized(t.AuthRequired, verifiedAuthServices)
}

func (t Tool) RequiresClientAuthorization(resourceMgr tools.SourceProvider) (bool, error) {
	source, err := tools.GetCompatibleSource[compatibleSource](resourceMgr, t.Source, t.Name, t.Kind)
	if err != nil {
		return false, err
	}
	return source.UseClientAuthorization(), nil
}

type DataScanItem struct {
	Name       string
	CreateTime time.Time
	State      string
}

type Response struct {
	DataScans []DataScanItem
}

func (t Tool) Invoke(ctx context.Context, resourceMgr tools.SourceProvider, params parameters.ParamValues, accessToken tools.AccessToken) (any, error) {
	source, err := tools.GetCompatibleSource[compatibleSource](resourceMgr, t.Source, t.Name, t.Kind)
	if err != nil {
		return nil, err
	}

	paramsMap := params.AsMap()
		
	project := paramsMap["project"].(string)
	if project == "" {
		project = source.BigQueryProject()
	}

	location := paramsMap["location"].(string)
	if location == "" {
		return nil, fmt.Errorf("location parameter is required")
	}

	state := paramsMap["state"].(string)
	
	pageSize := int32(paramsMap["pageSize"].(int))

	req := &dataplexpb.ListDataScansRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", project, location),
		OrderBy: "create_time desc",	
		PageSize: pageSize,	
	}

	if state != "" {
		req.Filter = fmt.Sprintf("state=\"%s\"", state)
	}

	fmt.Println("Request is: ", req) // Remove this log

	dataScanClient, dataplexClientCreator, _ := source.MakeDataplexDataScanClient()()

	if source.UseClientAuthorization() {
		tokenStr, err := accessToken.ParseBearerToken()
		if err != nil {
			return nil, fmt.Errorf("error parsing access token: %w", err)
		}
		_, dataScanClient, err = dataplexClientCreator(tokenStr)
		if err != nil {
			return nil, fmt.Errorf("error creating client from OAuth access token: %w", err)
		}
	}

	it := dataScanClient.ListDataScans(ctx, req)
	fmt.Printf("Listing DataScans in %s with state %s...\n", req.Parent, state)

	var resp Response


	for {
		dataScan, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterator.Next: %w", err)
		}

		item := DataScanItem{
			Name:       dataScan.GetName(),
			CreateTime: dataScan.GetCreateTime().AsTime(),
			State:      dataScan.GetState().String(),
		}
		resp.DataScans = append(resp.DataScans, item)
	}

	return resp, nil
}

func (t Tool) ParseParams(data map[string]any, claims map[string]map[string]any) (parameters.ParamValues, error) {
	// Parse parameters from the provided data
	return parameters.ParseParams(t.Parameters, data, claims)
}

func (t Tool) Manifest() tools.Manifest {
	// Returns the tool manifest
	return t.manifest
}

func (t Tool) McpManifest() tools.McpManifest {
	// Returns the tool MCP manifest
	return t.mcpManifest
}

func (t Tool) GetAuthTokenHeaderName(resourceMgr tools.SourceProvider) (string, error) {
	return "Authorization", nil
}