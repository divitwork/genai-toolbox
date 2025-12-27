---
title: "bigquery-get-data-scan-info"
type: docs
weight: 1
description: >
  A "bigquery-get-data-scan-info" tool is a foundational utility used to view detailed results and configurations of a specific data profile scan or insight generation scan.
---

## About

The `bigquery-get-data-scan-info` tool allows users to retrieve the full details of a specific Dataplex data scan. It provides comprehensive information, including the scan's display name, the associated data source, and the specific statistical results from the most recent profile run.

By accessing these insights, conversational agents can better understand table structures and data distributions to provide more accurate and grounded analytics responses.

`bigquery-get-data-scan-info` accepts the following parameter:

- **`name`** (required): The full resource name of the data scan to fetch (e.g., `projects/{project}/locations/{location}/dataScans/{datascan_id}`).

## User Prompts

Common prompts that trigger this tool include:
* "Get the details for the data scan 'my-table-profile'."
* "Show me the results of the latest profile scan for the transactions table."
* "What is the status and result of this specific Dataplex scan?"
* "Retrieve the statistical insights for the scan named projects/my-project/locations/us/dataScans/daily-scan."

## Requirements

### IAM Permissions
To view data scan details and results, the following role must be granted:
* **Dataplex DataScan Viewer** (`roles/dataplex.dataScanViewer`): Required on the project containing the data scan to view its configuration and results.

## Example

```yaml
tools:
  get_data_scan_info:
    kind: bigquery-get-data-scan-info
    source: bigquery-source
    description: Use this tool to view data profile scan and insight generation scan results.