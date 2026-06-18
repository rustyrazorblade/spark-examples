# spark-examples — setup TODO

Extracted from easy-db-lab. Outstanding work to make this a fully standalone repo.

## Build / CI
- [ ] Verify a clean build: `./gradlew build` (connector + common modules; bulk-writer modules need analytics first).
- [ ] Build Cassandra Analytics locally so the bulk-writer modules compile: `bin/build-cassandra-analytics` (requires JDK 11/17). Confirm the script's paths work from this repo root.
- [ ] Add a CI workflow (GitHub Actions) that builds analytics, then runs `./gradlew check`.

## Publish the fat (shadow) jars to GitHub
Goal: make the bulk-writer / connector shadow jars downloadable so easy-db-lab's `spark submit --jar <url|path>` can consume them without a local build.
- [ ] Decide the channel:
  - **GitHub Releases** (recommended for this use case): attach `*-all.jar` shadow jars as release assets on tag push. Stable download URLs; simplest for `spark submit`. Wire with a release workflow using `gh release create <tag> build/libs/*.jar` (or `softprops/action-gh-release`).
  - **GitHub Packages (Maven)**: publish via the `maven-publish` plugin to the GitHub Packages Maven registry. Better if consumed as a Maven dependency/coordinate; requires auth (PAT/`GITHUB_TOKEN`) on both publish and consume.
- [ ] Add the chosen publishing config to the shadow modules and a workflow trigger (tag or manual `workflow_dispatch`).
- [ ] Update easy-db-lab docs to point `spark submit` at the published jar location.

## Cleanup / docs
- [ ] Add a README (what these examples are, how to build, how to submit to EMR).
- [ ] Confirm `bin/upload-iam-bulk-writer.sh` paths are correct for this repo layout.
- [ ] Decide whether to keep the `awssdk` Kotlin version-catalog in settings.gradle (only the Java AWS SDK v2 `libs.awssdk.*` entries are actually used here).
