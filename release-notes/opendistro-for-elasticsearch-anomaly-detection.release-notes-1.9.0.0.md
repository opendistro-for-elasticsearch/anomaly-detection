## Version 1.9.0.0 Release Notes

Compatible with Elasticsearch 7.8.0 and Open Distro for Elasticsearch 1.9.0

## Enhancements

- delete deprecated getFeatureSamplesForPeriods getPreviewFeatures (PR [#151](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/151))
- remove deprecated getCurrentFeatures (PR [#152](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/152))
- not update threshold with zero scores (PR [#157](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/157))
- threshold model outputs zero grades instead of nan grades (PR [#158](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/158))
- configure rcf with new minimum samples (PR [#160](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/160))
- Rollover AD result index less frequently (PR [#168](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/168))
- Use aggregation's default value if no docs (PR [#167](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/167))
- Add setting for enabling/disabling circuit breaker (PR [#169](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/169))
- Add result indices retention period (PR [#174](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/174))

## Bug Fixes

- Fix retrying saving anomaly results (PR [#154](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/154))

## Infra Changes

- Add breaking changes in release notes (PR [#145](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/145))
- change rcf from local to maven (PR [#150](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/150))
- Run tests on docker if docker image exists (PR [#165](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/165))
- Add auto release drafter (PR [#178](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/178))

## Version Upgrades

- ODFE Release 1.8.1 (PR [#156](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/156))
- Bump ES version to 7.8.0 (PR [#172](https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/172))
