---
title: "bigquery-data-profile"
type: docs
weight: 1
description: >
  A "bigquery-data-profile" tool is a foundational utility that scans enterprise tables to compute rich statistical and semantic profiles—distributions, patterns, anomalies, and correlations.
---

## About

The `bigquery-data-profile` tool allows users to identify common statistical characteristics—such as common values, data distribution, and null counts—of the columns in BigQuery tables. This tool computes rich statistical and semantic profiles that power context-aware generation of table and column descriptions.

By feeding the Knowledge Engine to build connected data graphs, it helps conversational agents ground their analytics responses, effectively reducing hallucinations and improving factual accuracy.

## User Prompts

Common prompts that trigger this tool include:
* "Can you profile the transactions table and tell me what you find?"
* "What is the distribution of values in the product_category column of the products table?"
* "Show me the statistical summary for the website_traffic table."
* "Are there any columns with a high percentage of nulls in the orders table?"

## Requirements

### IAM Permissions
To manage and utilize data profile scans, the following roles must be granted based on the specific task:

#### Scan Management and Execution
*   **Dataplex DataScan Editor** (`roles/dataplex.dataScanEditor`): Required on the project containing the data scan to create, run, update, and delete scans.
*   **Dataplex DataScan Viewer** (`roles/dataplex.dataScanViewer`): Required on the project to view results, jobs, and history.

#### Service Account Permissions
The **Dataplex Universal Catalog service account** requires the following roles to process BigQuery data:
*   **BigQuery Job User** (`roles/bigquery.jobUser`): On the project running the scan.
*   **BigQuery Data Viewer** (`roles/bigquery.dataViewer`): On the tables being scanned.
*   **External Tables (Cloud Storage)**: To profile external tables, grant **Storage Object Viewer** (`roles/storage.objectViewer`) and **Storage Legacy Bucket Reader** (`roles/storage.legacyBucketReader`) on the Cloud Storage bucket.

#### Output and Publication
*   **Exporting Results**: **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target table.
*   **Publishing to Catalog**: **Dataplex Catalog Editor** (`roles/dataplex.catalogEditor`) on the `@bigquery` entry group.
*   **Viewing Published Results**: **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the table to see results in the BigQuery "Data profile" tab.

## Example

```yaml
tools:
  data_profile:
    kind: bigquery-data-profile
    source: bigquery-source
    description: Use this tool to generate statistical profiles, unique counts, and value distributions for BigQuery tables.
