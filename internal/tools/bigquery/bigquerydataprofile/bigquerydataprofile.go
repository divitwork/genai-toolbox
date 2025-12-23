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
	"strings"

	dataplexapi "cloud.google.com/go/dataplex/apiv1"
	dataplexpb "cloud.google.com/go/dataplex/apiv1/dataplexpb"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	bigqueryds "github.com/googleapis/genai-toolbox/internal/sources/bigquery"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/googleapis/genai-toolbox/internal/util/parameters"
)

const kind string = "bigquery-data-profile"

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
	// Add parameters....
	// prompt := parameters.NewStringParameter("prompt", "Prompt representing search intention. Do not rewrite the prompt.")
	// datasetIds := parameters.NewArrayParameterWithDefault("datasetIds", []any{}, "Array of dataset IDs.", parameters.NewStringParameter("datasetId", "The IDs of the bigquery dataset."))
	// projectIds := parameters.NewArrayParameterWithDefault("projectIds", []any{}, "Array of project IDs.", parameters.NewStringParameter("projectId", "The IDs of the bigquery project."))
	// types := parameters.NewArrayParameterWithDefault("types", []any{}, "Array of data types to filter by.", parameters.NewStringParameter("type", "The type of the data. Accepted values are: CONNECTION, POLICY, DATASET, MODEL, ROUTINE, TABLE, VIEW."))
	// pageSize := parameters.NewIntParameterWithDefault("pageSize", 5, "Number of results in the search page.")
	params := parameters.Parameters{/** add parameters */}

	description := "Use this tool to analyze and understand tables by generating statistical insights."
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

type Response struct {
	DisplayName   string
	Description   string
	Type          string
	Resource      string
	DataplexEntry string
}

var typeMap = map[string]string{
	"bigquery-connection":  "CONNECTION",
	"bigquery-data-policy": "POLICY",
	"bigquery-dataset":     "DATASET",
	"bigquery-model":       "MODEL",
	"bigquery-routine":     "ROUTINE",
	"bigquery-table":       "TABLE",
	"bigquery-view":        "VIEW",
}

func ExtractType(resourceString string) string {
	lastIndex := strings.LastIndex(resourceString, "/")
	if lastIndex == -1 {
		// No "/" found, return the original string
		return resourceString
	}
	return typeMap[resourceString[lastIndex+1:]]
}

func (t Tool) Invoke(ctx context.Context, resourceMgr tools.SourceProvider, params parameters.ParamValues, accessToken tools.AccessToken) (any, error) {
	source, err := tools.GetCompatibleSource[compatibleSource](resourceMgr, t.Source, t.Name, t.Kind)
	if err != nil {
		return nil, err
	}

	paramsMap := params.AsMap()
	_ = paramsMap
	// pageSize := int32(paramsMap["pageSize"].(int))
	// prompt, _ := paramsMap["prompt"].(string)
	// projectIdSlice, err := parameters.ConvertAnySliceToTyped(paramsMap["projectIds"].([]any), "string")
	// if err != nil {
	// 	return nil, fmt.Errorf("can't convert projectIds to array of strings: %s", err)
	// }
	// projectIds := projectIdSlice.([]string)
	// datasetIdSlice, err := parameters.ConvertAnySliceToTyped(paramsMap["datasetIds"].([]any), "string")
	// if err != nil {
	// 	return nil, fmt.Errorf("can't convert datasetIds to array of strings: %s", err)
	// }
	// datasetIds := datasetIdSlice.([]string)
	// typesSlice, err := parameters.ConvertAnySliceToTyped(paramsMap["types"].([]any), "string")
	// if err != nil {
	// 	return nil, fmt.Errorf("can't convert types to array of strings: %s", err)
	// }
	// types := typesSlice.([]string)

	req := &dataplexpb.CreateDataScanRequest{
		// Required: Parent location resource name extracted from your project/location
		Parent: "projects/autopush-cmek-test-project-1/locations/us-west1",
		// Required: Scan ID set to "testscan"
		DataScanId: "testscan1",
		// Required: DataScan resource configuration
		DataScan: &dataplexpb.DataScan{
			DisplayName: "TestScan",
			// Required: The BigQuery table resource identified in your "Table to scan" section
			Data: &dataplexpb.DataSource{
				Source: &dataplexpb.DataSource_Resource{
					Resource: "//bigquery.googleapis.com/projects/autopush-cmek-test-project-1/datasets/TestDSUsWest1/tables/TestTabUsWest1",
				},
			},
			// Required: Execution settings (mapped from the "Schedule" UI section)
			ExecutionSpec: &dataplexpb.DataScan_ExecutionSpec{
				Trigger: &dataplexpb.Trigger{
					Mode: &dataplexpb.Trigger_OnDemand_{
						OnDemand: &dataplexpb.Trigger_OnDemand{},
					},
				},
			},
			// Required: Settings for the Data Profile scan type
			Spec: &dataplexpb.DataScan_DataProfileSpec{
				DataProfileSpec: &dataplexpb.DataProfileSpec{
					// Mapped from the "Sampling size" UI parameter
					SamplingPercent: 10.0,
				},
			},
		},
	}

	fmt.Println("Request is: ", req)

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

	op, err := dataScanClient.CreateDataScan(ctx, req)
	if err != nil {
		fmt.Println("Error1 is: ", err)
		return nil, fmt.Errorf("failed to create data scan for project %q", source.BigQueryProject())
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		fmt.Println("Error2 is: ", err)
		return nil, fmt.Errorf("failed to create data scan for project %q", source.BigQueryProject())
	}

	fmt.Println("respose is %s", resp)
	
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