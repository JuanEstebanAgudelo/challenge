# DataOps Globant Challenge

[![PyPI - Version](https://img.shields.io/pypi/v/dataops-globant-challenge.svg)](https://pypi.org/project/dataops-globant-challenge)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dataops-globant-challenge.svg)](https://pypi.org/project/dataops-globant-challenge)

-----

## Table of Contents

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install dataops-globant-challenge
```

## License

`dataops-globant-challenge` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.


## How to Run

# For Step 1

```bash
spark-submit --conf spark.jars.packages="com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11" src/dataops_globant_challenge/section_1.py
```

# For Step 2

```bash
spark-submit src/dataops_globant_challenge/section_2.py
```


