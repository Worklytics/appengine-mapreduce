# README - java

Java library for MapReduce implementation.  


## Usage

Packages are hosted in [GitHub Packages](https://github.com/Worklytics/appengine-mapreduce/packages). Specific
 instructions to use these:
 
  1. Ensure you authenticate Maven such that it can obtain packages from GitHub. See [Authenticating to GitHub
   Packages](https://help.github.com/en/github/managing-packages-with-github-packages/configuring-apache-maven-for-use-with-github-packages#authenticating-to-github-packages).
  2. Include our GitHub package repository in your `pom.xml`:
```xml
   <repositories>
     <repository>
       <id>github</id>
       <name>Apache Maven Packages by Worklytics</name>
       <url>https://maven.pkg.github.com/worklytics/packages</url>
     </repository>
   </repositories>
```
  3. Specifically include our package in your `pom.xml`:
```xml
<dependency>
  <groupId>com.google.appengine.tools</groupId>
  <artifactId>appengine-mapreduce</artifactId>
  <version>0.10-snapshot</version>
</dependency>
```
 

## Build Instructions

Only Maven is currently supported. The build relies on packages hosted in GitHub package registry; you must have a
 Maven `settings.xml` properly set-up to authenticate you with GitHub. See [Authenticating to GitHub Packages](https
 ://help.github.com/en/github/managing-packages-with-github-packages/configuring-apache-maven-for-use-with-github-packages#authenticating-to-github-packages);
 

To test the library:
```shell script
mvn test
```

To build binary for distribution:
```shell script
mvn build
```

To deploy (to GitHub)

```shell script
mvn deploy
```
