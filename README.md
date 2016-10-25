pdblp
=====

This is a simple interface to integrate pandas and the Bloomberg Open API.
The standard Bloomberg API provides an extensive set of features for building
applications on top of however does not provide easy and interactive access to
data. This package provides several functions for accessing historical market
data and reference data. A simple set of examples is available
[here](https://matthewgilbert.github.io/pdblp/tutorial.html).
In theory this should work on both Windows and Linux connecting to Bloomberg
Server API however this has only been tested using Windows and the Desktop API.

The library borrows heavily from a similar package available
[here](https://github.com/kyuni22/pybbg)

## Requires

[Bloomberg Open API](http://www.bloomberglabs.com/api/) 

[pandas](http://pandas.pydata.org/)

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

## Documentation
Once `pdblp` has been installed, documentation can be built using

```
cd doc
make html
```

and then viewed in ./doc/_build. Before building this ensure that you are
logged into a Bloomberg terminal as this is required for building many of the
examples.

An online version of the documentation can be view at https://matthewgilbert.github.io/pdblp/
