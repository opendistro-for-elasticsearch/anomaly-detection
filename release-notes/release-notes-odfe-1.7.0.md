## Open Distro for Elasticsearch 1.7.0 Release Notes
Compatible with Elasticsearch 7.6.1 and Open Distro for Elasticsearch 1.7.0.

## Initial Release
The Anomaly Detection Elasticsearch plugin enables you to detect anomalies in streaming time series data based on the random cut forest (RCF) algorithm.
You can create anomaly detectors and add features to them to customize what data you want to detect anomalies over.

This plugin can be used with the [Anomaly Detection Kibana plugin](https://github.com/opendistro-for-elasticsearch/anomaly-detection-kibana-plugin) for an intuitive user interface that can be used to configure, start, and stop anomaly detectors. You can also view the anomaly history for all of your created detectors. 

You can use the plugin with the same version of the [Open Distro Alerting plugin](https://github.com/opendistro-for-elasticsearch/alerting) and [Open Distro Alerting Kibana plugin](https://github.com/opendistro-for-elasticsearch/alerting-kibana-plugin) to get alert notifications. You can create a monitor based on an anomaly detector directly on the Alerting Kibana plugin. Monitors run checks on the anomaly detection results regularly and trigger alerts based on custom trigger conditions.

## Features

1. Create and configure anomaly detectors over user-specified indices and features
2. Start and stop detectors at any time
3. Query anomaly results
4. Query existing detectors
5. Query specific detector details and current state
6. Preview anomaly results for new detector features

## Current Limitations

- Limit of 1000 detectors per Elasticsearch cluster
- Limit of 5 features per detector
- Total detectors memory limit of 10% of JVM heap
- Not all API calls have complete error handling
- We will continuously add new unit test cases, but we don't have 100% unit test coverage for now. This is a great area for developers from the community to contribute and help improve test coverage
- Please see documentation links and GitHub issues for other details