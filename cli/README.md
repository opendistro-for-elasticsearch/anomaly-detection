![AD CLI Test and Build](https://github.com/opendistro-for-elasticsearch/anomaly-detection/workflows/AD%20CLI%20Test%20and%20Build/badge.svg)
# Open Distro for Elasticsearch AD CLI

The AD CLI component in Open Distro for Elasticsearch (ODFE) is a command line interface for ODFE AD plugin.
This CLI provides greater flexibility of use. User can use CLI to easily do things that are difficult or sometimes impossible to do with kibana UI. This doesn’t use any additional  system resources to load any of graphical part, thus making it simpler and faster than UI. 

It only supports [Open Distro for Elasticsearch (ODFE) AD Plugin](https://opendistro.github.io/for-elasticsearch-docs/docs/ad/)
You must have the ODFE AD plugin installed to your Elasticsearch instance to connect. 
Users can run this CLI from MacOS, Windows, Linux and connect to any valid Elasticsearch end-point such as Amazon Elasticsearch Service (AES).The ESAD CLI implements AD APIs.

## Version Compatibility Matrix

| ESAD Version  | ODFE Version        |
| ------------- |:-------------------:|
| 0.1           | 1.7.X, 1.8.X, 1.9.X |

## Features

* Create Detectors
* Start, Stop, Delete Detectors
* Create named profiles to connect to ES cluster

## Install

Launch your local Elasticsearch instance and make sure you have the Open Distro for Elasticsearch AD plugin installed.

To install the AD CLI:


1. Install from source:

    ```
    $ go get github.com/opendistro-for-elasticsearch/anomaly-detection/cli
    ```

## Configure

Before using the ESAD CLI, you need to configure your ESAD credentials. You can do this in several ways:

* Configuration command
* Config file

The quickest way to get started is to run the `esad profile create`

```
$ esad profile create
Enter profile's name: dev
ES Anomaly Detection Endpoint: https://localhost:9200
ES Anomaly Detection User: admin
ES Anomaly Detection Password:
```
Make sure profile name is unique within config file. `create` command will not allow user to create duplicate profile.

To use a config file, create a YAML file like this
```
profiles:
- endpoint: https://localhost:9200
  username: admin
  password: foobar
  name: default
- endpoint: https://odfe-node1:9200
  username: admin
  password: foobar
  name: dev
```
and place it on ~/.esad/config.yaml.
If you wish to place the shared config file in a different location than the one specified above, you need to tell esad where to find it. Do this by setting the appropriate environment variable:

```
export ESAD_CONFIG_FILE=/path/to/config_file
```
You can have multiple profiles defined in the configuration file.
You can then specify which profile to use by using the --profile option. `default` profile will be used if profile parameter is skipped.



## Basic Commands

An ESAD CLI has following structure
```
$ esad <command> <subcommand> [flags and parameters]
```
For example to start detector:
```
$ esad start [detector-name-pattern]
```
To view help documentation, use one of the following:
```
$ esad --help
$ esad <command> --help
$ esad <command> <subcommand> --help
```
To get the version of the ESAD CLI:
```
$ esad --version
```

## Getting Help

The best way to interact with our team is through GitHub. You can open an [issue](https://github.com/opendistro-for-elasticsearch/anomaly-detection/issues) and tag accordingly.

## Copyright

Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

