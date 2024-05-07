# wrabbit

![](https://github.com/rabix/wrabbit/workflows/Tests/badge.svg)
[![PyPI version](https://badge.fury.io/py/wrabbit.svg)](https://pypi.org/project/wrabbit/)

Library for creating wrapper applications for Nextflow apps compatible with any Seven Bridges powered platform.

## Installation
`wrabbit` needs Python 3.7 or later

### Install latest release on pypi
```bash
pip install wrabbit
```

### Install latest (unreleased) code
```bash
pip install git+https://github.com/rabix/wrabbit.git
```

# Nextflow parser
```python
from wrabbit.parser.nextflow import NextflowParser

workflow_path = '/path/to/app-directory'
parsed_nextflow = NextflowParser(workflow_path)
```
