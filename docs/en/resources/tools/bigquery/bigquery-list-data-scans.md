---
title: "bigquery-list-data-scans"
type: docs
weight: 1
description: >
  A "bigquery-list-data-scans" tool is a utility that allows users to retrieve and filter a list of Dataplex data scans within a specific Google Cloud project and location.
---

## About

The `bigquery-list-data-scans` tool provides a way to discover existing data scans (such as data profile or data quality scans) in a project. This is useful for auditing existing configurations, finding a specific scan's resource name, or monitoring the state of multiple scans at once.

The tool returns a list of scans including their resource names, creation times, and current states.

`bigquery-list-data-scans` accepts the following parameters:

- **`location`** (required): The Google Cloud region to search for data scans.
- **`project`** (optional): The Google Cloud project ID. If not provided, it defaults to the project from the source configuration.
- **`state`** (optional): Filters scans by their current state (e.g., `ACTIVE`, `CREATING`). If omitted, all states are returned.
- **`pageSize`** (optional): The number of results to return per page (default is 5).

## User Prompts

Common prompts that trigger this tool include:
* "List all the data scans in the us-central1 region."
* "Show me all active data scans for my project."
* "What data scans have been created recently in europe-west1?"

## Requirements

### IAM Permissions
To list data scans, you must have the following role or equivalent permissions on the project:
* **Dataplex DataScan Viewer** (`roles/dataplex.dataScanViewer`): Required to list and view data scan resources.

## Example

```yaml
tools:
  list_data_scans:
    kind: bigquery-list-data-scans
    source: bigquery-source
    description: Use this tool to get a list of data scans for a project and location.
ss