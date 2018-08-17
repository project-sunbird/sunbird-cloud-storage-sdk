# Sunbird Cloud Storage sdk

Sunbird Cloud Storage is a multi-cloud sdk that gives you the freedom to create applications that are portable across clouds.

## Getting Started

### Prerequisites

Below are the required maven dependencies to be included in your project pom.xml

```
	<dependency>
		<groupId>org.scala-lang</groupId>
		<artifactId>scala-library</artifactId>
		<version>2.11.8</version>
	</dependency>
	<dependency>
		<groupId>com.fasterxml.jackson.core</groupId>
		<artifactId>jackson-annotations</artifactId>
		<version>2.6.5</version>
	</dependency>
	<dependency>
		<groupId>com.fasterxml.jackson.core</groupId>
		<artifactId>jackson-databind</artifactId>
		<version>2.6.5</version>
	</dependency>
	<dependency>
		<groupId>com.fasterxml.jackson.module</groupId>
		<artifactId>jackson-module-scala_2.11</artifactId>
		<version>2.6.5</version>
	</dependency>
	<dependency>
		<groupId>org.apache.httpcomponents</groupId>
		<artifactId>httpclient</artifactId>
		<version>4.5.2</version>
	</dependency>
	<dependency>
		<groupId>io.netty</groupId>
    	<artifactId>netty</artifactId>
		<version>3.7.0.Final</version>
	</dependency>
```

## Installation

* **Step 1:**

	Download Sunbird Cloud Storage project from [github](https://github.com/project-sunbird/sunbird-cloud-storage-sdk.git)

* **Step 2:**

	Build Sunbird Cloud Storage project and add `sunbird-cloud-store-sdk-1.0.jar` to your project classpath

* **Step 3:**

	Add the below maven dependency to your project pom.xml:

	```
	<dependency>
		<groupId>org.sunbird</groupId>
		<artifactId>sunbird-cloud-store-sdk</artifactId>
		<version>1.0</version>
	</dependency>
	```

## Usage

Refer [this](https://github.com/project-sunbird/sunbird-cloud-storage-sdk/blob/analytics-migration/src/main/scala/org/sunbird/cloud/storage/IStorageService.scala) for all available APIs and detailed usage.

* **Azure storage example:**

	```scala
	import org.sunbird.cloud.storage.factory.{ StorageConfig, StorageServiceFactory }

	val storageService = StorageServiceFactory.getStorageService(StorageConfig("azure", "<account-name>", "<account-key>"))

	storageService.upload("<container-name>", "<file-to-upload>", "<objectKey>")

	```

* **S3 storage example:**

	```scala
	import org.sunbird.cloud.storage.factory.{ StorageConfig, StorageServiceFactory }

	val storageService = StorageServiceFactory.getStorageService(StorageConfig("s3", "<aws-key>", "<aws-secret>"))

	storageService.upload("<bucket-name>", "<file-to-upload>", "<objectKey>")

	```
