# High Cardinaltiy support in Anomaly Detection RFC

The purpose of this request for comments (RFC) is to introduce our plan to enhance Anamaly Detection for OpenDistro by adding the support of high cardinality. This RFC is meant to cover the high level functionality of the high cardinality support and doesn’t go into implementation details and architecture.

## Problem Statement

Currently the Anomaly Detection for Elasticsearch for OpenDistro only support single entity use case. (e.g. average of cpu usage across all hosts, instead of cpu usage of individual hosts). For multi entity cases, currently users have to create individual detectors for each entity manually. It is very time consuming, and could simply become infeasible when the number of entities reach to hundreds or thousands (high cardinality).

## Proposed solution

We propose to create a new type of detector to support multi entity use case. With this feature, users only need to create one single detector to cover all entities that can be categorized by one or multiple fields. They will also be able to view the results of the anomaly detection in one unified report.

### Create Detector

Most of the detector creation workflow is similar to the single entity detectors, the only additional input is a categorical field, e.g. ip_address, which will be used to split data into multiple entities. We’ll start with supporting only one categorical fields. We’ll add support of multiple categorical fields in future releases.

### Anomaly Report

The output of multi entity detector will be categorized by entities. The entities with most anomalies detected will be presented in a heatmap plot. Users then have the option to click into each entity for more details about the anomalies. 

### Entity capacity

Supporting high cardinality with multiple entities definitely takes more resource than single entity detectors. The total number of supported unique entities depends on the cluster configuration. We'll provide a table with the launch to show the recommended number of entities for certain cluster configurations. In general we are planning to support up to 10K entities in the initial release. 

## Providing Feedback

If you have comments or feedback on our plans for Multi Entity support for Anomaly Detection, please comment on the [original GitHub issue](https://github.com/opendistro-for-elasticsearch/anomaly-detection/issues/xxx) in this project to discuss.
