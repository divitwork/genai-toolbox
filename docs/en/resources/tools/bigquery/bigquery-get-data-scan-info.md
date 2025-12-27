---
title: "bigquery-get-data-scan-info"
type: docs
weight: 1
description: >
  A "bigquery-get-data-scan-info" tool is a foundational utility that is used to view data profile scan and insight generation scan results.
---

## About

The `"bigquery-get-data-scan-info"` tool allows users to view data profile scan and insight generation scan results. 

By feeding the Knowledge Engine to build connected data graphs, it helps conversational agents ground their analytics responses, effectively reducing hallucinations and improving factual accuracy.

`bigquery-get-data-scan-info"` accepts the following parameters:

- **`name`** (required): The resource name of the data scan you want to fetch.

## Example

```yaml
tools:
  data_profile:
    kind: bigquery-get-data-scan-info
    source: bigquery-source
    description: Use this tool to view data profile scan and insight generation scan results.