# What is this?

A collection of utilities for use in DataHub ingestion configurations:

- **rename_urn** transformer that overrides the URN of all aspects it sees

# How do I use it?

From the CLI, simply install it into your Python environment.

From the web UI, add this repo as an Extra Pip Library under the advanced settings of an ingestion configuration, pinning it to a specific hash:

```
["git+https://github.com/ligfx/datahub-ingestion-utils.git@somehash"]
```