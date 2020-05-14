## Open Distro for Elasticsearch Anomaly Detection Plugin 1.7.0.0 Release Notes

Compatible with Elasticsearch 7.6.1 and Open Distro for Elasticsearch 1.7.0.

## Initial Release

The Open Distro for Elasticsearch Anomaly Detection plugin enables you to detect anomalies in streaming time series data based on the random cut forest (RCF) algorithm.

You can create anomaly detectors and add features to them to customize what data you want to detect anomalies over.

This plugin can be used with the [Anomaly Detection Kibana plugin](https://github.com/opendistro-for-elasticsearch/anomaly-detection-kibana-plugin) for an intuitive user interface that can be used to configure, start, and stop anomaly detectors. You can also view the anomaly history for all of your created detectors. 

This plugin works independently. You can also use the plugin with the same version of the [Open Distro for Elasticsearch Alerting Kibana plugin](https://github.com/opendistro-for-elasticsearch/alerting-kibana-plugin) to get alert notifications. You can create a monitor based on an anomaly detector directly on the Alerting Kibana plugin. Monitors run checks on the anomaly detection results regularly and trigger alerts based on custom trigger conditions.

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

## Major Changes
* Add state and error to profile API [PR #84](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/84)
* Preview detector on the fly [PR #72](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/72)
* Cancel query if given detector already have one [PR #54](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/54)
* Support return AD job when get detector [PR #50](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/50)
* Add AD job on top of JobScheduler [PR #44](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/44)
* Adding negative cache to throttle extra request [PR #40](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/40)
* Add window delay support [PR #24](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/24)
* Circuit breaker [PR #10](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/10) [PR #7](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/7)
* Stats collection [PR #8](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/8)

## Enhancements
* Stats API: moved detector count call outside transport layer and make asynchronous [PR #108](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/108)
* Change AD result index rollover setting [PR #100](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/100)
* Add async maintenance [PR #94](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/94)
* Add async stopModel [PR #93](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/93)
* Add timestamp to async putModelCheckpoint [PR #92](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/92)
* Add async clear [PR #91](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/91)
* Use callbacks and bug fix [PR #83](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/83)
* Add async trainModel [PR #81](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/81)
* Add async getColdStartData [PR #80](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/80)
* Change the default value of lastUpdateTime [PR #77](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/77)
* Add async getThresholdingResult [PR #70](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/70)
* Add async getRcfResult [PR #69](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/69)
* Fix rcf random seed in preview [PR #68](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/68)
* Fix empty preview result due to insufficient sample [PR #65](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/65)
* Add async CheckpointDao methods. [PR #62](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/62)
* Record error and execution start/end time in AD result; handle except… [PR #59](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/59)
* Improve log message when we cannot get anomaly result [PR #58](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/58)
* Write detection code path in callbacks [PR #48](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/48)
* Send back error response when failing to stop detector [PR #45](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/45)
* Adjust preview configuration for more data [PR #39](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/39)
* Refactor using ClientUtil [PR #32](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/32)
* Return empty preview results on failure [PR #31](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/31)
* Allow non-negative window delay [PR #30](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/30)
* Return no data error message to preview [PR #29](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/29)
* Change AD setting name [PR #26](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/26)
* Add async CheckpointDao methods. [PR #17](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/17)
* Add async implementation of getFeaturesForSampledPeriods. [PR #16](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/16)
* Add async implementation of getFeaturesForPeriod. [PR #15](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/15)
* Add test evaluating anomaly results [PR #13](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/13)

## Bug Fixes
* Change setting name so that rpm/deb has the same name as zip [PR #109](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/109)
* Can't start AD job if detector has no feature [PR #76](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/76)
* Fix null pointer exception during preview [PR #74](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/74)
* Add threadpool prefix and change threadpool name [PR #56](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/56)
* Change setting name and fix stop AD request [PR #41](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/41)
* Revert "merge changes from alpha branch: change setting name and fix … [PR #38](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/38)
* Fix stop detector api to use correct request [PR #25](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/25)

## Infra Changes
* Add release notes for ODFE 1.7.0 [PR #120](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/120) [PR #119](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/119)
* Open Distro Release 1.7.0 [PR #106](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/106)
* Create opendistro-elasticsearch-anomaly-detection.release-notes.md [PR #103](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/103)
* Update test branch [PR #101](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/101)
* Bump opendistroVersion to 1.6.1 [PR #99](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/99)
* Change to mention we support only JDK 13 [PR #98](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/98)
* AD opendistro 1.6 support [PR #87](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/87)
* Added URL for jb_scheduler-plugin_zip instead of local file path [PR #82](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/82)
* Change build instruction for JDK [PR #61](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/61)
* ODFE 1.4.0 [PR #43](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/43)
* Add spotless for code format [PR #22](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/22)
* Update third-party [PR #14](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/14)
* Build artifacts for rpm, deb, zip [PR #5](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/5)
* Update test-workflow.yml [PR #2](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/2)
