# Map Reduce for Google App Engine

Library for running MapReduce on Google App Engine

## Usage via Maven

 1. Add Worklytics org package repository:
    ```xml
      <repository>
        <id>worklytics</id>
        <name>Apache Maven Packages Published by Worklytics</name>
        <url>https://maven.pkg.github.com/Worklytics</url>
      </repository>
    ```
 2. Find the latest package at [Worklytics's package repository](https://github.com/Worklytics/appengine-mapreduce/packages) and add dependency to
  your `pom.xml`:
    ```xml
    <dependency>
      <groupId>co.worklytics</groupId>
      <artifactId>appengine-mapreduce</artifactId>
      <version>0.10-SNAPSHOT</version> <!-- look this up --> 
    </dependency>
    ```


## Building

Only Maven is currently supported.

### Testing
First, set up an env variable `APPENGINE_MAPREDUCE_CI_SERVICE_ACCOUNT_KEY` as base64-encoded copy of a GCP Service
 Account key. The service account must have admin permissions for GCS in a project for which GCS APIs are enabled. 
   - in IntelliJ, can be set via your Run Configurations for JUnit
   - in CI, we use a GitHub secret for this; see [.github/workflows/test-java.yml]
 
Then, too test the library:
```shell script
mvn test
```
### Distribution

To build binary for distribution:
```shell script
mvn package
```
without tests (which take a long time):
```shell script
mvn package -Dmaven.test.skip=TRUE
```

To deploy it:

 1. create a GitHub personal access token and put it in your `/.m2/settings.xml`, as described in [GitHub's docs](https://help.github.com/en/github/managing-packages-with-github-package-registry/configuring-apache-maven-for-use-with-github-package-registry)
 2. run the following (from the `java/` subdirectory of the repo):
 ```shell script
mvn deploy
```
