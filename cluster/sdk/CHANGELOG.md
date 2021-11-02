# Changelog

## [v0.2.12](https://github.com/feast-dev/feast-spark/tree/v0.2.12) (2021-08-19)
[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.11...v0.2.12)

**Implemented enhancements:**
- Keep only unique entity rows in entity dataframe in historical retrieval job [\#93](https://github.com/feast-dev/feast-spark/pull/93) ([pyalex](https://github.com/pyalex))
- Add checkpoint in each iteration of join loop (historical retrieval job) to truncate logical plan [\#92](https://github.com/feast-dev/feast-spark/pull/92) ([pyalex](https://github.com/pyalex))
- Add persistent checkpoint in historical retrieval [\#91](https://github.com/feast-dev/feast-spark/pull/91) ([pyalex](https://github.com/pyalex))
- Omit skewness in spark join by broadcasting entity dataframe [\#90](https://github.com/feast-dev/feast-spark/pull/90) ([pyalex](https://github.com/pyalex))

## [v0.2.11](https://github.com/feast-dev/feast-spark/tree/v0.2.11) (2021-08-16)
[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.10...v0.2.11)

**Implemented enhancements:**
- Speed-up join in historical retrieval by replacing pandas with native spark [\#89](https://github.com/feast-dev/feast-spark/pull/89) ([pyalex](https://github.com/pyalex))

## [v0.2.10](https://github.com/feast-dev/feast-spark/tree/v0.2.10) (2021-08-13)
[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.9...v0.2.10)

**Implemented enhancements:**
- Optimize historical retrieval by filtering rows only within timestamp boundaries from entity dataframe [\#87](https://github.com/feast-dev/feast-spark/pull/87) ([pyalex](https://github.com/pyalex))

**Fixed bugs:**

- Pandas is missing in spark job base image [\#88](https://github.com/feast-dev/feast-spark/pull/88) ([pyalex](https://github.com/pyalex))
- Do not use gcThreshold for historical retrieval jobs [\#86](https://github.com/feast-dev/feast-spark/pull/86) ([pyalex](https://github.com/pyalex))

## [v0.2.9](https://github.com/feast-dev/feast-spark/tree/v0.2.9) (2021-07-28)
[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.8...v0.2.9)

**Fixed bugs:**
- Refresh redis topology on write [\#83](https://github.com/feast-dev/feast-spark/pull/83) ([khorshuheng]
- Use hash for schedule job id [\#80](https://github.com/feast-dev/feast-spark/pull/80) ([khorshuheng]

**Implemented enhancements:**
- Make max inflight RPCs in bigtable client configurable through spark config [\#81](https://github.com/feast-dev/feast-spark/pull/81) ([pyalex])
- Use gcThresholdSec label instead of max_age if present [\#82](https://github.com/feast-dev/feast-spark/pull/82) ([pyalex])

## [v0.2.8](https://github.com/feast-dev/feast-spark/tree/v0.2.8) (2021-07-07)
[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.7...v0.2.8)

**Implemented enhancements:**
- Use entity source project dataset for bigquery view creation [\#79](https://github.com/feast-dev/feast-spark/pull/79) ([khorshuheng])(https://github.com/khorshuheng))

## [v0.2.7](https://github.com/feast-dev/feast-spark/tree/v0.2.7) (2021-06-15)
[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.6...v0.2.7)

**Implemented enhancements:**
- Extra configuration for bigtable client [\#78](https://github.com/feast-dev/feast-spark/pull/78) ([pyalex])(https://github.com/pyalex))
- Add configuration for bigquery entity staging location [\#77](https://github.com/feast-dev/feast-spark/pull/77) ([khorshuheng])(https://github.com/khorshuheng))


## [v0.2.6](https://github.com/feast-dev/feast-spark/tree/v0.2.6) (2021-05-20)
[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.5...v0.2.6)

**Fixed bugs:**
- Fix broken scheduled batch ingestion jobs [\#72](https://github.com/feast-dev/feast-spark/pull/72) ([khorshuheng](https://github.com/khorshuheng))

## [v0.2.5](https://github.com/feast-dev/feast-spark/tree/v0.2.5) (2021-05-11)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.4...v0.2.5)

**Fixed bugs:**

- Fix SSTable name length restrictions during ingestion [\#70](https://github.com/feast-dev/feast-spark/pull/70) ([terryyylim](https://github.com/terryyylim))

**Merged pull requests:**

- Test long entity names in e2e tests [\#71](https://github.com/feast-dev/feast-spark/pull/71) ([pyalex](https://github.com/pyalex))

## [v0.2.4](https://github.com/feast-dev/feast-spark/tree/v0.2.4) (2021-05-05)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.3...v0.2.4)

**Fixed bugs:**

- Batch Ingestion job is stuck after exception thrown [\#69](https://github.com/feast-dev/feast-spark/pull/69) ([pyalex](https://github.com/pyalex))

**Merged pull requests:**

- Update redis host for e2e tests [\#68](https://github.com/feast-dev/feast-spark/pull/68) ([khorshuheng](https://github.com/khorshuheng))
- Bigtable sink \(row level\) metrics [\#67](https://github.com/feast-dev/feast-spark/pull/67) ([pyalex](https://github.com/pyalex))

## [v0.2.3](https://github.com/feast-dev/feast-spark/tree/v0.2.3) (2021-04-29)
**Implemented enhancements:**

- Support for Schedule Spark Application [\#59](https://github.com/feast-dev/feast-spark/pull/59) ([khorshuheng](https://github.com/khorshuheng))

**Fixed bugs:**

- Call admin.modifyTable only if table spec changes: preventing quota to be exceeded [\#66](https://github.com/feast-dev/feast-spark/pull/66) ([pyalex](https://github.com/pyalex))

**Merged pull requests:**

- Explicitly stop sparkSession on exception to prevent it stucking [\#65](https://github.com/feast-dev/feast-spark/pull/65) ([pyalex](https://github.com/pyalex))
- Migrate sparkop e2e tests to kf-feast cluster [\#62](https://github.com/feast-dev/feast-spark/pull/62) ([khorshuheng](https://github.com/khorshuheng))

## [v0.2.2](https://github.com/feast-dev/feast-spark/tree/v0.2.2) (2021-04-27)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.1...v0.2.2)

**Implemented enhancements:**

- Add project whitelist [\#57](https://github.com/feast-dev/feast-spark/pull/57) ([terryyylim](https://github.com/terryyylim))
- Configurable triggering interval from streaming ingestion [\#63](https://github.com/feast-dev/feast-spark/pull/63) ([pyalex](https://github.com/pyalex))

**Merged pull requests:**

- Bump spark version to 3.0.2 [\#60](https://github.com/feast-dev/feast-spark/pull/60) ([pyalex](https://github.com/pyalex))

## [v0.2.1](https://github.com/feast-dev/feast-spark/tree/v0.2.1) (2021-04-19)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.2.0...v0.2.1)

**Implemented enhancements:**

- Cassandra as sink option for ingestion job [\#51](https://github.com/feast-dev/feast-spark/pull/51) ([khorshuheng](https://github.com/khorshuheng))

**Merged pull requests:**

- Reuse stencil client between Spark Tasks [\#58](https://github.com/feast-dev/feast-spark/pull/58) ([pyalex](https://github.com/pyalex))

## [v0.2.0](https://github.com/feast-dev/feast-spark/tree/v0.2.0) (2021-04-15)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.1.2...v0.2.0)

**Implemented enhancements:**

- Cassandra as sink option for ingestion job [\#51](https://github.com/feast-dev/feast-spark/pull/51) ([khorshuheng](https://github.com/khorshuheng))
- Bigtable as alternative Online Storage [\#46](https://github.com/feast-dev/feast-spark/pull/46) ([pyalex](https://github.com/pyalex))

**Fixed bugs:**

- Cassandra schema update should not fail when column exist [\#55](https://github.com/feast-dev/feast-spark/pull/55) ([khorshuheng](https://github.com/khorshuheng))
- Fix avro serialization: reuse generated schema in advance [\#52](https://github.com/feast-dev/feast-spark/pull/52) ([pyalex](https://github.com/pyalex))

**Merged pull requests:**

- Fix dependencies for aws e2e [\#56](https://github.com/feast-dev/feast-spark/pull/56) ([pyalex](https://github.com/pyalex))
- Better handling for invalid proto messages [\#54](https://github.com/feast-dev/feast-spark/pull/54) ([pyalex](https://github.com/pyalex))
- Bump feast version to 0.9.5 [\#53](https://github.com/feast-dev/feast-spark/pull/53) ([terryyylim](https://github.com/terryyylim))
- Fix setup.py dependencies [\#49](https://github.com/feast-dev/feast-spark/pull/49) ([woop](https://github.com/woop))
- Add documentation building for readthedocs.org [\#47](https://github.com/feast-dev/feast-spark/pull/47) ([woop](https://github.com/woop))
- Cleanup Scala code [\#28](https://github.com/feast-dev/feast-spark/pull/28) ([YikSanChan](https://github.com/YikSanChan))

## [v0.1.2](https://github.com/feast-dev/feast-spark/tree/v0.1.2) (2021-03-17)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.1.1...v0.1.2)

**Implemented enhancements:**

- Implicit type conversion for entity and feature table source [\#43](https://github.com/feast-dev/feast-spark/pull/43) ([khorshuheng](https://github.com/khorshuheng))

**Merged pull requests:**

- Make field mapping for batch source consistent with streaming source [\#45](https://github.com/feast-dev/feast-spark/pull/45) ([khorshuheng](https://github.com/khorshuheng))
- Better cleanup for cached rdds in Streaming Ingestion [\#44](https://github.com/feast-dev/feast-spark/pull/44) ([pyalex](https://github.com/pyalex))
- Add tests for master branch [\#25](https://github.com/feast-dev/feast-spark/pull/25) ([woop](https://github.com/woop))

## [v0.1.1](https://github.com/feast-dev/feast-spark/tree/v0.1.1) (2021-03-11)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.1.0...v0.1.1)

## [v0.1.0](https://github.com/feast-dev/feast-spark/tree/v0.1.0) (2021-03-08)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/92043a1f67043f531ab76ac2093d0ca604afd825...v0.1.0)

**Fixed bugs:**

- Fix GH Actions PR Workflow Support for PRs from External Contributors [\#35](https://github.com/feast-dev/feast-spark/pull/35) ([mrzzy](https://github.com/mrzzy))
- Allow only project filter when listing ft [\#32](https://github.com/feast-dev/feast-spark/pull/32) ([terryyylim](https://github.com/terryyylim))


**Implemented enhancements:**

- Configurable retry for failed jobs \(JobService Control Loop\) [\#29](https://github.com/feast-dev/feast-spark/pull/29) ([pyalex](https://github.com/pyalex))


**Merged pull requests:**

- Add Helm chart publishing for Feast Spark [\#24](https://github.com/feast-dev/feast-spark/pull/24) ([woop](https://github.com/woop))
- Add project filter to list jobs [\#16](https://github.com/feast-dev/feast-spark/pull/16) ([terryyylim](https://github.com/terryyylim))

- Add protos and update CI [\#23](https://github.com/feast-dev/feast-spark/pull/23) ([pyalex](https://github.com/pyalex))
- Add SparkClient as separate pytest fixture [\#22](https://github.com/feast-dev/feast-spark/pull/22) ([pyalex](https://github.com/pyalex))
- Update CI workflow [\#19](https://github.com/feast-dev/feast-spark/pull/19) ([terryyylim](https://github.com/terryyylim))
- Update submodule [\#18](https://github.com/feast-dev/feast-spark/pull/18) ([terryyylim](https://github.com/terryyylim))
- Use feast core images from GCR instead of building them ourselves [\#17](https://github.com/feast-dev/feast-spark/pull/17) ([pyalex](https://github.com/pyalex))
- Build & push docker workflow for spark ingestion job [\#13](https://github.com/feast-dev/feast-spark/pull/13) ([pyalex](https://github.com/pyalex))
- Fix Azure e2e tests [\#11](https://github.com/feast-dev/feast-spark/pull/11) ([oavdeev](https://github.com/oavdeev))
- fix submodule ref [\#10](https://github.com/feast-dev/feast-spark/pull/10) ([oavdeev](https://github.com/oavdeev))