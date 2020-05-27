## Open Distro for Elasticsearch Anomaly Detection Plugin 1.8.0.0 Release Notes

Compatible with Elasticsearch 7.7.0 and Open Distro for Elasticsearch 1.8.0.

## New Features
* Add settings to disable/enable AD dynamically (#105) [PR #127](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/127)

## Enhancements
* Ultrawarm integration [PR #125](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/125)
* Add shingle size, total model size, and model's hash ring to profile API [PR #128](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/128)
* Prevent creating detector with duplicate name. [PR #134](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/134)

## Bug Fixes
* Fix that AD job cannot be terminated due to missing training data [PR #126](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/126)
* Fix incorrect detector count in stats APIs [PR #129](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/129)
* fix dynamic setting of max detector/feature limit [PR #130](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/130)

## Infra Changes
* Add CI/CD workflows [PR #133](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/133)
* Use spotless to manage license headers and imports [PR #136](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/136)

