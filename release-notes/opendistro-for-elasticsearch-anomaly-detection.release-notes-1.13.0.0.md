## Version 1.13.0.0 Release Notes

Compatible with Elasticsearch 7.10.2

### Features

* add AD task and tune detector&AD result data model ([#329](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/329))
* add AD task cache ([#337](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/337))
* start historical detector ([#355](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/355))
* stop historical detector; support return AD task in get detector API ([#359](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359))
* update/delete historical detector;search AD tasks ([#362](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/362))
* add user in AD task ([#370](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/370))

### Enhancements

* Adding unit tests for Transport Actions ([#327](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/327))
* Adding role based filtering for rest of APIs ([#325](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/325))
* add ad task stats ([#332](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/332))
* Adding support for Security Test Framework ([#331](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/331))
* filter out exceptions which should not be counted in failure stats ([#341](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/341))
* Moving Preview Anomaly Detectors to Transport layer ([#321](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/321))
* Adding role based filtering for Preview API ([#356](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/356))
* change the backend role filtering to keep consistent with alerting plugin ([#383](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/383))

### Bug Fixes

* Fix the profile API returns prematurely. ([#340](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/340))
* Fix another case of the profile API returns prematurely ([#353](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/353))
* Fix log messages and init progress for the profile API ([#374](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/374))
* validate detector only when start detector; fix flaky test case ([#377](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/377))

### Infrastructure

* add IT cases for filtering out non-server exceptions for HC detector ([#348](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/348))
* rename rpm/deb artifact name ([#371](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/371))
* fix flaky test case ([#376](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/376))
* Change release workflow to use new staging bucket for artifacts ([#358](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/358))
* Update draft release notes config ([#379](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/379))

### Documentation

* Updating Readme to include Secure tests ([#334](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/334))
* remove spotless header file; remove copyright year in new files for h… ([#372](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/372))
* Add release notes for version 1.13.0.0 ([#382](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/382))

### Maintenance

* upgrade to ES 7.10.2 ([#378](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/378))
