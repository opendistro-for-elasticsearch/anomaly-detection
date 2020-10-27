## Version 1.11.0.0 Release Notes

Compatible with Elasticsearch 7.9.1

### Enhancements

* remove deprecated code ([#228](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/228))
* CLI: Download Detector Configuration as File ([#229](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/229))
* CLI: Update Display Strings ([#231](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/231))
* CLI: Fix build errors ([#235](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/235))
* Adding RestActions support for AD Search Rest API's ([#234](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/234))
* Adding RestActions support for Detector Stats API ([#237](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/237))
* add anomaly feature attribution to model output ([#232](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/232))
* CLI: Update Detector Configurations ([#233](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/233))
* Adding RestActions support Delete Detector API ([#238](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/238))
* Adding RestActions support for Get Detector API ([#242](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/242))
* Adding RestActions support Create/Update Detector API ([#243](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/243))
* Modifying RestAction for Execute API ([#246](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/246))
* Adding RestActions support for Start/Stop Detector API ([#244](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/244))
* Verifying multi-entity detectors ([#240](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/240))
* Selectively store anomaly results when index pressure is high ([#241](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/241))
* Adding User support for Detector and DetectorJob ([#251](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/251))
* Add support filtering the data by one categorical variable ([#270](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/270))
* Added User support for background job and API Transport Actions ([#272](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/272))
* suppport HC detector in profile api ([#274](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/274))
* Auto flush checkpoint queue if too many are waiting ([#279](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/279))
* Renaming all Actions to distinguish between internal and external facing ([#284](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/284))
* Adding new Search Detector Info API ([#286](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/286))
* Updating Search Detector API to reject all other queries other than BoolQuery ([#288](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/288))
* Adding detector to Create/Update detector response ([#289](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/289))
* set user as nested type to support exists query ([#291](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/291))
* fix no permission when preview detector with detector id ([#294](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/294))
* update user role filter to use nested query ([#293](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/293))
* Injecting User role for background job ([#295](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/295))

### Bug fixes

* Fix nested field issue ([#277](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/277))
* Upgrade mapping ([#278](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/278))
* Fix issue where max number of multi-entity detector doesn't work for UpdateDetector ([#285](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/285))
* Fix for stats API ([#287](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/287))
* Fix for get detector API ([#290](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/290))

### Infrastructure

* Ignoring flaky test ([#255](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/255))
* Adding common-utils dependency from Maven ([#280](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/280))
* Fix build issue of common util ([#281](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/281))
* Exclude IndexAnomalyDetectorResponse from Jacoco to unblock build ([#292](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/292))

### Documentation

* CLI: Update Display Strings ([#231](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/231))
* Add release notes for 1.11.0.0 ([#276](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/276))


### Maintenance

* upgrade rcf libaries ([#239](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/239))
* bump ad plugin version to 1.11.0 ([#275](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/275))
