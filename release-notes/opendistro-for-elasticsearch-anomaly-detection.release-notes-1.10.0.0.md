## Version 1.10.0.0 Release Notes

Compatible with Elasticsearch 7.9.0

### Features

 * AD CLI ([#196](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/196))
 * Get detector ([#207](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/207))

### Enhancements

* change to exhausive search for training data ([#184](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/184))
* Adds initialization progress to profile API ([#164](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/164))
* Queries data from the index when insufficient data in buffer to form a full shingle ([#176](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/176))
* Adding multinode integration test support ([#201](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/201))
* Change Profile List Format ([#206](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/206))
* Improve ux ([#209](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/209))
* Allows window size to be set per detector. ([#203](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/203))
* not return estimated minutes remaining until cold start is finished ([#210](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/210))
* minor edits to the short and long text strings ([#211](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/211))
* Change to use callbacks in cold start ([#208](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/208))
* fix job index mapping ([#212](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/212))

### Infrastructure

* Use goimports instead of gofmt ([#214](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/214))
* Install go get outside cli directory ([#216](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/216))

### Documentation

* Automate release notes to unified standard ([#191](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/191))
* Add badges to AD ([#199](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/199))
* Test code coverage ([#202](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/202))

### Maintenance

* Upgrade from 1.9.0 to 1.10.0 ([#215](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/215))