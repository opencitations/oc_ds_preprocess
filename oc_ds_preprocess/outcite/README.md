# OUTCITE Data Processing Pipeline

Pipeline for preparing citations and metadata from the [OUTCITE](https://demo-outcite.gesis.org/) project for ingestion into [OpenCitations Index](https://index.opencitations.net/) (citations) and [OpenCitations Meta](https://meta.opencitations.net/) (metadata).

## Overview

The OUTCITE project has provided the [Open Social Science Citation Index (OpenSSCI)](10.5281/zenodo.18172741), published on Zenodo in the form of two tables, one for metadata and one for citations.

The pipeline validates the tables and cleans them by removing invalid rows.


## Usage

The simplest way to prepare OUTCITE data is to run the automatic, no-configuration workflow with [`oc_pruner`](https://pypi.org/project/oc-pruner/). This validates the tables and automatically removes all rows containing issues of any kind, then ensures that there is a transitive closure between the entities in the two tables (all entities in the metadata table are involved in a citation; all the entities involved in citations have associated metadata).

```
oc_pruner pipeline \
  -m <path/to/metadata.csv> \
  -c <path/to/citations.csv> \
  -o <path/to/output_directory>
```

To have more control over the rows to remove (for example, ignoring specific errors), it is possible to use a specific configuration and the `prune` command. For such use, refer to the documentation of `oc_pruner`.