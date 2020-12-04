## Version 1.12.0.0 Release Notes

Compatible with Elasticsearch 7.10.0

### Enhancements

* Improve profile API ([#298](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/298))
* Add checkpoint index retention for multi entity detector ([#283](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/283))
* Stashing context for Stats API to allow users to query from RestAPI ([#300](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/300))
* add HC detector request/failure stats ([#307](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/307))

### Bug Fixes

* Fix edge case where entities found for preview is empty ([#296](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/296))
* fix null user in detector ([#301](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/301))
* fix fatal error of missing method parseString ([#302](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/302))
* remove clock Guice binding ([#305](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/305))
* filter out empty value for entity features ([#306](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/306))
* Fix for upgrading mapping ([#309](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/309))
* fix double nan error when parse to json ([#310](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/310))
* Fix issue where data hole exists for Preview API ([#312](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/312))
* fix delete running detector bug ([#320](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/320))
* fix detector and feature serialization ([#322](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/322))
* Moving common-utils to 1.12.0.2 ([#323](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/323))

### Infrastructure
* Add multi node integration testing into CI workflow ([#318](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/318))

### Maintenance
* Support ES 7.10.0 ([#313](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/313))
