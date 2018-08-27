pdblp
=====
[![PyPI version](https://badge.fury.io/py/pdblp.svg)](https://badge.fury.io/py/pdblp)

This is a simple interface to integrate pandas and the Bloomberg Open API.
The standard Bloomberg API provides an extensive set of features for building
applications on top of however does not provide easy and interactive access to
data. This package provides several functions for accessing historical market
data and reference data. A simple set of examples is available
[here](https://matthewgilbert.github.io/pdblp/tutorial.html).

The library borrows heavily from a similar package available
[here](https://github.com/kyuni22/pybbg)

## Requires

`python 3.x`

[Bloomberg Open API](http://www.bloomberglabs.com/api/)

[pandas](http://pandas.pydata.org/)

and for `pdblp.parser`

[pyparsing](https://pythonhosted.org/pyparsing/) >= 2.2.0

## Installation
You can install from PyPi using

```
pip install pdblp
```

or you can clone this repository and pip install the package, i.e.

```
git clone https://github.com/matthewgilbert/pdblp.git
pip install -e pdblp
```

`blpapi` can be installed directly from the above link or you can install from
the conda channel dsm into an environment, e.g.

```
conda install -n py36 -c dsm blpapi
```

If going this route make sure there is a build available for your version of
python https://anaconda.org/dsm/blpapi/files

If you are getting `'GLIBCXX_3.4.21' not found` error when attempting to import
`blpapi`, the following appears to resolve this issue.

```
conda install libgcc
```

## Documentation

The documentation can be viewed at https://matthewgilbert.github.io/pdblp/

### Bloomberg Documentation

For general documentation on the Bloomberg API check out the Developer's Guide.
For documentation on relevant Bloomberg fields for accessing data, check out
the Reference Guide: Services and Schemas. To access these, from a
Bloomberg Terminal go `WAPI <GO>` -> `API Developer's Guide`.

### Building the documentation

The documentation relies on [Sphinx](http://www.sphinx-doc.org/en/master/).
Building the documentation can be done by setting up a conda environment using
`conda create --name pdblp_doc --file doc-environment.yml`, sourcing this
environment and then installing the relevant version of `pdblp`. Documentation
can then be built using

```
cd doc
make html
```

and viewed in ./doc/_build. Before building this ensure that you are
logged into a Bloomberg terminal as this is required for building many of the
examples.
