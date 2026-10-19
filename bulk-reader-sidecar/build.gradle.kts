// Bulk-read benchmark: reads tables through Cassandra Sidecar with the Cassandra Analytics bulk
// reader and runs Spark SQL query shapes over them.
//
// Built against the Spark 4 / Scala 2.13 / JDK 17 build of Cassandra Analytics, which matches the
// EMR release easy-db-lab uses (emr-spark-8.0.0: Spark 4.0.2, Scala 2.13, Corretto 17 and 21).
// The java and Spark compileOnly setup comes from the root build.gradle.kts subprojects block.

plugins {
    application
    id("com.gradleup.shadow")
}

val scalaVersion: String by extra
val sparkVersion: String by extra
val cassandraAnalyticsVersion: String by rootProject.extra

java {
    // Cassandra Analytics 0.5 is compiled for JDK 17.
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

dependencies {
    // The bulk reader.  The core jar carries the per-version bridges (four-zero, five-zero,
    // six-zero) as nested jars and loads them in their own class loader.
    implementation("org.apache.cassandra:cassandra-analytics-core_spark4_$scalaVersion:$cassandraAnalyticsVersion")

    testImplementation(libs.junit.jupiter.all)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
}

application {
    mainClass.set("com.rustyrazorblade.easydblab.spark.BulkReadBenchmark")
}

tasks.test {
    useJUnitPlatform()
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("bulk-reader-sidecar")
    archiveClassifier.set("")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "com.rustyrazorblade.easydblab.spark.BulkReadBenchmark"
    }
}

tasks.named<Jar>("jar") {
    enabled = false
    dependsOn(tasks.named("shadowJar"))
}
