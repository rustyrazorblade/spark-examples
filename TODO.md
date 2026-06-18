# spark-examples — setup TODO

Extracted from easy-db-lab. Remaining work to round out the standalone repo.

## Build / CI — done
- [x] Verify a clean build (`./gradlew check`) — green in CI.
- [x] Build Cassandra Analytics in CI (`bin/build-cassandra-analytics`, JDK 17).
- [x] CI workflow (`.github/workflows/ci.yml`) — build + test on push/PR.

## Publish the fat jars — done
- [x] Publish via **GitHub Releases**: `.github/workflows/release.yml` builds the four job
      shadow jars and attaches them to the release on a `v*` tag (artifact on manual dispatch).
      First release: https://github.com/rustyrazorblade/spark-examples/releases/tag/v0.1.0
- [x] easy-db-lab docs/test-plans updated to download the published jars.

## Remaining
- [ ] Add a README (what these examples are, how to build, how to submit to EMR, jar download URLs).
- [ ] Bump the marketplace actions off Node 20 (checkout/setup-java/upload-artifact/setup-gradle/action-gh-release) — CI warns but still runs.
- [ ] Confirm `bin/upload-iam-bulk-writer.sh` paths are correct for this repo layout (or drop it in favor of release downloads).
- [ ] Decide whether to keep the `awssdk` Kotlin version-catalog in settings.gradle (only the Java AWS SDK v2 `libs.awssdk.*` entries are used here).
