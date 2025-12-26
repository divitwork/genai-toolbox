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
	location := parameters.NewStringParameter("location", "This refers to a Google Cloud region.")
	dataset := parameters.NewStringParameter("dataset", "Specifies the dataset of the table.")
	table := parameters.NewStringParameter("table", "The name of the table for which to to run data profilce scan.")
	displayname := parameters.NewStringParameter("displayname", "The name and id of datascan. If not provided, the agent would generate a unique name/id based on timestamp.")
	project := parameters.NewStringParameterWithDefault("project", "", "The Google Cloud project ID. If not provided, the tool defaults to the project from the source configuration.")

	params := parameters.Parameters{location, dataset, table, displayname, project}

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
		
	project := paramsMap["project"].(string)
	if project == "" {
		project = source.BigQueryProject()
	}

	location := paramsMap["location"].(string)
	if location == "" {
		return nil, fmt.Errorf("location parameter is required")
	}

	dataset := paramsMap["dataset"].(string)
	if dataset == "" {
		return nil, fmt.Errorf("dataset parameter is required")
	}

	table := paramsMap["table"]
	if table == "" {
		return nil, fmt.Errorf("table parameter is required")
	}

	displayName := paramsMap["displayname"].(string)
	dataScanID := displayName

	// Construct the parent resource name
	parent := fmt.Sprintf("projects/%s/locations/%s", project, location)

	// Construct the BigQuery table resource name
	bqResource := fmt.Sprintf("//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s", project, dataset, table)

	req := &dataplexpb.CreateDataScanRequest{
		Parent:  parent,
		DataScanId: dataScanID,
		DataScan: &dataplexpb.DataScan{
			DisplayName: displayName,
			Data: &dataplexpb.DataSource{
				Source: &dataplexpb.DataSource_Resource{
					Resource: bqResource,
				},
			},
			ExecutionSpec: &dataplexpb.DataScan_ExecutionSpec{
				Trigger: &dataplexpb.Trigger{
					Mode: &dataplexpb.Trigger_OnDemand_{
						OnDemand: &dataplexpb.Trigger_OnDemand{},
					},
				},
			},
			Spec: &dataplexpb.DataScan_DataProfileSpec{
				DataProfileSpec: &dataplexpb.DataProfileSpec{
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

	runReq := &dataplexpb.RunDataScanRequest{
		Name: resp.GetName(), 
	}

	fmt.Println("Run dataScan req: ", runReq)

	runResp, err := dataScanClient.RunDataScan(ctx, runReq)
	if err != nil {
		fmt.Errorf("failed to run data scan: %v", err)
	}

	fmt.Println("Run DataScan resp: ", runResp)

	job := runResp.GetJob()
	if job != nil {
		fmt.Printf("Successfully started Job: %s\n", job.GetName())
		fmt.Printf("Current Job State: %s\n", job.GetState().String())
		fmt.Printf("Job unique ID: %s\n", job.GetUid())
	}
	
	return job, nil
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