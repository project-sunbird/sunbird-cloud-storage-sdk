# Implementation Plan: Multi-Module Java 11 Cloud Storage SDK

## Overview

Refactor the Sunbird Cloud Storage SDK from a single Scala/jclouds-based module into a **multi-module pure Java 11 Maven project** with CSP-specific implementations using official cloud SDKs. Replaces all existing Scala source code.

### Key Decisions
- **Ceph S3**: Folded into the AWS module via configurable endpoint override (no separate module)
- **OCI**: Uses official OCI Java SDK (`oci-java-sdk-objectstorage`) for native API and Instance Principal auth
- **Existing code**: Replaced entirely — clean break at version 2.0.0

---

## 1. Maven Module Structure

### Directory Layout
```
sunbird-cloud-storage-sdk/
├── pom.xml                                    (parent POM, packaging=pom)
├── cloud-storage-sdk-api/
│   ├── pom.xml
│   └── src/main/java/org/sunbird/cloud/storage/
│       ├── IStorageService.java               (main interface)
│       ├── StorageConfig.java                 (config with Builder pattern)
│       ├── StorageServiceFactory.java         (ServiceLoader-based factory)
│       ├── StorageServiceProvider.java        (SPI interface)
│       ├── AbstractStorageService.java        (shared orchestration logic)
│       ├── model/
│       │   ├── Blob.java                      (data model)
│       │   └── DeleteTarget.java              (replaces Scala tuple)
│       ├── exception/
│       │   └── StorageServiceException.java   (runtime exception)
│       └── util/
│           ├── DateRangeUtil.java             (java.time replacement for Joda-Time)
│           └── FileUtil.java                  (file copy, zip extraction)
├── cloud-storage-sdk-aws/
│   ├── pom.xml
│   └── src/
│       ├── main/java/org/sunbird/cloud/storage/service/aws/
│       │   ├── AwsStorageService.java         (AWS S3 + Ceph S3 implementation)
│       │   └── AwsStorageServiceProvider.java (SPI registration)
│       └── main/resources/META-INF/services/
│           └── org.sunbird.cloud.storage.StorageServiceProvider
├── cloud-storage-sdk-azure/
│   ├── pom.xml
│   └── src/
│       ├── main/java/org/sunbird/cloud/storage/service/azure/
│       │   ├── AzureStorageService.java
│       │   └── AzureStorageServiceProvider.java
│       └── main/resources/META-INF/services/
│           └── org.sunbird.cloud.storage.StorageServiceProvider
├── cloud-storage-sdk-gcp/
│   ├── pom.xml
│   └── src/
│       ├── main/java/org/sunbird/cloud/storage/service/gcp/
│       │   ├── GcpStorageService.java
│       │   └── GcpStorageServiceProvider.java
│       └── main/resources/META-INF/services/
│           └── org.sunbird.cloud.storage.StorageServiceProvider
└── cloud-storage-sdk-oci/
    ├── pom.xml
    └── src/
        ├── main/java/org/sunbird/cloud/storage/service/oci/
        │   ├── OciStorageService.java
        │   └── OciStorageServiceProvider.java
        └── main/resources/META-INF/services/
            └── org.sunbird.cloud.storage.StorageServiceProvider
```

### Artifact Summary

| Module | Artifact ID | Key Dependencies |
|--------|-------------|------------------|
| Parent | `cloud-storage-sdk-parent` | — (pom packaging) |
| API | `cloud-storage-sdk-api` | SLF4J, Tika Core, Jackson Databind |
| AWS | `cloud-storage-sdk-aws` | API module + AWS SDK v2 (s3, sts) |
| Azure | `cloud-storage-sdk-azure` | API module + azure-storage-blob, azure-identity |
| GCP | `cloud-storage-sdk-gcp` | API module + google-cloud-storage |
| OCI | `cloud-storage-sdk-oci` | API module + oci-java-sdk-objectstorage |

**Group ID**: `org.sunbird` (unchanged)
**Version**: `2.0.0` (major bump — breaking change from Scala to Java)

---

## 2. Parent POM Configuration

```xml
<properties>
    <java.version>11</java.version>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

    <!-- Central SDK version management -->
    <aws.sdk.version>2.25.0</aws.sdk.version>
    <azure.storage.blob.version>12.25.0</azure.storage.blob.version>
    <azure.identity.version>1.11.0</azure.identity.version>
    <gcp.storage.version>2.30.0</gcp.storage.version>
    <oci.sdk.version>3.30.0</oci.sdk.version>
    <tika.version>2.9.0</tika.version>
    <jackson.version>2.15.3</jackson.version>
    <slf4j.version>2.0.9</slf4j.version>
    <junit5.version>5.10.1</junit5.version>
    <mockito.version>5.8.0</mockito.version>
</properties>
```

Uses `<dependencyManagement>` with BOM imports for AWS (`software.amazon.awssdk:bom`), Azure (`com.azure:azure-sdk-bom`), and GCP (`com.google.cloud:libraries-bom`).

---

## 3. Interface Design (Java)

### 3.1 Scala-to-Java Type Mapping

| Scala Type | Java Type | Notes |
|-----------|----------|-------|
| `Option[Boolean]` | `boolean` parameter + method overloads with defaults | Convenience `default` methods |
| `Option[Int]` | `Integer` (nullable) | Null = not specified |
| `Option[String]` | `String` (nullable) | Null = not specified |
| `List[Blob]` | `java.util.List<Blob>` | Immutable via `List.of()` / `Collections.unmodifiableList()` |
| `List[(String, Boolean)]` | `java.util.List<DeleteTarget>` | New typed record instead of tuple |
| `Future[List[String]]` | `CompletableFuture<List<String>>` | Standard Java async |
| `Array[Byte]` | `byte[]` | Direct |
| `Map[String, AnyRef]` | `Map<String, Object>` | Standard Java |
| `Option[Array[Byte]]` | `byte[]` (nullable) | Null = no payload |

### 3.2 IStorageService Interface

```java
public interface IStorageService extends AutoCloseable {

    // Upload
    String upload(String container, String file, String objectKey,
                  boolean isDirectory, int attempt, int retryCount, Integer ttl);
    default String upload(String container, String file, String objectKey) { ... }

    CompletableFuture<List<String>> uploadFolder(String container, String file,
                  String objectKey, boolean isPublic, Integer ttl,
                  Integer retryCount, int attempt);
    default CompletableFuture<List<String>> uploadFolder(String container,
                  String file, String objectKey) { ... }

    String put(String container, byte[] content, String objectKey,
               boolean isPublic, boolean isDirectory, Integer ttl, Integer retryCount);
    default String put(String container, byte[] content, String objectKey) { ... }

    // Signed URLs
    String getSignedURL(String container, String objectKey, Integer ttl, String permission);
    default String getSignedURL(String container, String objectKey) { ... }

    String getSignedURLV2(String container, String objectKey, Integer ttl,
                          String permission, String contentType,
                          Map<String, String> additionalParams);
    default String getSignedURLV2(String container, String objectKey) { ... }

    // Download
    void download(String container, String objectKey, String localPath, boolean isDirectory);
    default void download(String container, String objectKey, String localPath) { ... }

    // Delete
    void deleteObject(String container, String objectKey, boolean isDirectory);
    default void deleteObject(String container, String objectKey) { ... }
    void deleteObjects(String container, List<DeleteTarget> objectKeys);

    // Copy
    void copyObjects(String fromContainer, String fromKey,
                     String toContainer, String toKey, boolean isDirectory);
    default void copyObjects(String from, String fKey, String to, String tKey) { ... }

    // Archive
    void extractArchive(String container, String objectKey, String toKey);

    // Object access
    Blob getObject(String container, String objectKey, boolean withPayload);
    default Blob getObject(String container, String objectKey) { ... }
    List<String> getObjectData(String container, String objectKey);
    List<Blob> listObjects(String container, String prefix, boolean withPayload);
    default List<Blob> listObjects(String container, String prefix) { ... }
    List<String> listObjectKeys(String container, String prefix);

    // Search
    List<Blob> searchObjects(String container, String prefix,
                             String fromDate, String toDate,
                             Integer delta, String pattern);
    List<String> searchObjectKeys(String container, String prefix,
                                  String fromDate, String toDate,
                                  Integer delta, String pattern);

    // Path operations
    List<String> getPaths(String container, List<Blob> objects);
    String getUri(String container, String prefix, boolean isDirectory);
    default String getUri(String container, String prefix) { ... }

    // Lifecycle
    @Override void close();
}
```

### 3.3 StorageConfig (Builder pattern with enums)

```java
public final class StorageConfig {

    public enum AuthType {
        ACCESS_KEY,
        OIDC,                // NEW: primary auth for cloud-native deployments
        IAM,
        IAM_ROLE,
        INSTANCE_PROFILE
    }

    public enum StorageType {
        AWS, AZURE, GCLOUD, OCI, CEPHS3
    }

    // Fields: type, storageKey, storageSecret, endPoint, region, authType
    // Builder: StorageConfig.builder(StorageType.AWS).authType(AuthType.OIDC).build()
}
```

### 3.4 Models

**Blob.java** — Immutable data class with: `key`, `contentLength`, `lastModified`, `metadata`, `payload` (nullable byte[])

**DeleteTarget.java** — Replaces `(String, Boolean)` tuple with: `objectKey`, `isDirectory`. Static factories: `DeleteTarget.file(key)`, `DeleteTarget.directory(key)`

### 3.5 StorageServiceFactory (ServiceLoader discovery)

```java
public final class StorageServiceFactory {
    public static IStorageService getStorageService(StorageConfig config) {
        ServiceLoader<StorageServiceProvider> loader =
            ServiceLoader.load(StorageServiceProvider.class);
        for (StorageServiceProvider provider : loader) {
            if (provider.supports(config.getType())) {
                return provider.create(config);
            }
        }
        throw new StorageServiceException("No provider found for: " + config.getType());
    }
}
```

### 3.6 StorageServiceProvider (SPI interface)

```java
public interface StorageServiceProvider {
    boolean supports(StorageConfig.StorageType type);
    IStorageService create(StorageConfig config);
}
```

Each CSP module registers its provider via `META-INF/services/org.sunbird.cloud.storage.StorageServiceProvider`.

---

## 4. AbstractStorageService (Shared Logic in API Module)

Migrates orchestration logic from `BaseStorageService.scala` to Java. This is ~50% of BaseStorageService.

### Logic kept in abstract base (CSP-independent):
- `filesList()` — recursive file listing
- `uploadFolder()` — parallel upload via CompletableFuture + ForkJoinPool
- `upload()` — retry loop orchestration calling abstract `putObject()`
- `put()` — retry loop calling abstract `putContent()`
- `download()` — directory download orchestration calling abstract `downloadObject()`
- `deleteObject()` — delegates to `deleteObjects()`
- `searchObjects()` / `searchObjectKeys()` — date range iteration via `DateRangeUtil`
- `extractArchive()` — download → unzip → upload orchestration
- `listObjects()` — iterates `listObjectKeys` + `getObject`
- `copyObjects()` — directory copy orchestration calling abstract `copyObject()`

### Abstract methods each CSP must implement:

```java
// Container management
protected abstract void ensureContainerExists(String container);

// Blob I/O
protected abstract String putObject(String container, String objectKey, File file);
protected abstract String putObject(String container, String objectKey, byte[] content);
protected abstract InputStream getObjectStream(String container, String objectKey);
protected abstract String getObjectUri(String container, String objectKey);
protected abstract BlobDetail getObjectDetail(String container, String objectKey);

// Listing
protected abstract List<String> listKeys(String container, String prefix);

// Delete & Copy
protected abstract void removeObjects(String container, List<String> objectKeys);
protected abstract void copyObject(String fromContainer, String fromKey,
                                    String toContainer, String toKey);

// Signed URLs
protected abstract String generateSignedGetUrl(String container, String objectKey,
                                                int ttlSeconds);
protected abstract String generateSignedPutUrl(String container, String objectKey,
                                                int ttlSeconds, String contentType,
                                                Map<String, String> additionalParams);

// HDFS path prefix (e.g., "s3n://", "wasb://...", "gs://")
protected abstract String getHdfsPrefix(String container);
```

### Utility classes (in API module):

**DateRangeUtil.java**: Replaces Joda-Time with `java.time.LocalDate` / `DateTimeFormatter` / `ChronoUnit`.

**FileUtil.java**: `listFilesRecursive()`, `copyStream()`, `unZip()` — ported from `CommonUtil.scala`.

---

## 5. CSP Module Implementations

### 5.1 AWS Module (`cloud-storage-sdk-aws`)

**SDK**: AWS SDK v2 (`software.amazon.awssdk:s3`, `software.amazon.awssdk:sts`)

**Key class**: `AwsStorageService extends AbstractStorageService`

**Credential resolution by AuthType**:
| AuthType | AWS Credential Provider |
|----------|------------------------|
| `ACCESS_KEY` | `StaticCredentialsProvider.create(AwsBasicCredentials.create(key, secret))` |
| `OIDC` | `StsWebIdentityTokenFileCredentialsProvider` (reads `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`) |
| `IAM` / `IAM_ROLE` / `INSTANCE_PROFILE` | `DefaultCredentialsProvider.create()` |

**Ceph S3 support**: When `StorageType.CEPHS3`, builds `S3Client` with:
- `endpointOverride(URI.create(config.getEndPoint()))`
- `S3Configuration.builder().pathStyleAccessEnabled(true).build()`

**Signed URLs**: Uses `S3Presigner` with `presignGetObject()` / `presignPutObject()`

**HDFS prefix**: `s3n://<container>/`

**AwsStorageServiceProvider**: `supports()` returns `true` for `StorageType.AWS` and `StorageType.CEPHS3`

### 5.2 Azure Module (`cloud-storage-sdk-azure`)

**SDK**: `com.azure:azure-storage-blob`, `com.azure:azure-identity`

Replaces the old `com.microsoft.azure:azure-storage:5.0.0` with the modern Azure SDK for Java.

**Key class**: `AzureStorageService extends AbstractStorageService`

**Credential resolution by AuthType**:
| AuthType | Azure Credential |
|----------|-----------------|
| `ACCESS_KEY` | `StorageSharedKeyCredential(accountName, accountKey)` |
| `OIDC` | `WorkloadIdentityCredentialBuilder` (reads `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`) |
| `IAM` / `INSTANCE_PROFILE` | `ManagedIdentityCredentialBuilder` |

**Signed URLs**: Uses SAS (Shared Access Signatures):
- Access key auth: `blobClient.generateSas(sasValues)`
- OIDC/IAM auth: User Delegation SAS via `blobServiceClient.getUserDelegationKey()` + `blobClient.generateUserDelegationSas()`

**HDFS prefix**: `wasb://<container>@<storageKey>.blob.core.windows.net/`

### 5.3 GCP Module (`cloud-storage-sdk-gcp`)

**SDK**: `com.google.cloud:google-cloud-storage`

The current GCP implementation already works around jclouds by using native GCP SDK for signed URLs and raw HTTP for `listObjectKeys` and `download`. The new module eliminates jclouds entirely and uses native SDK for everything.

**Key class**: `GcpStorageService extends AbstractStorageService`

**Credential resolution by AuthType**:
| AuthType | GCP Credential |
|----------|---------------|
| `ACCESS_KEY` | `ServiceAccountCredentials.fromPkcs8()` using `additionalParams` from config, or `ServiceAccountCredentials.fromStream()` |
| `OIDC` | `ExternalAccountCredentials` via Workload Identity Federation (`GOOGLE_APPLICATION_CREDENTIALS` env var) |
| `IAM` / `INSTANCE_PROFILE` | `GoogleCredentials.getApplicationDefault()` / `ComputeEngineCredentials` |

**Signed URLs** (ported from current `GcloudStorageService.getPutSignedURL()`):
```java
storage.signUrl(blobInfo, ttl, TimeUnit.SECONDS,
    Storage.SignUrlOption.httpMethod(method),
    Storage.SignUrlOption.withV4Signature());
```
Supports chunked/resumable uploads via `HttpMethod.POST` + `x-goog-resumable: start` header (matching current implementation).

**HDFS prefix**: `gs://<container>/`

### 5.4 OCI Module (`cloud-storage-sdk-oci`)

**SDK**: `com.oracle.oci.sdk:oci-java-sdk-objectstorage` (native OCI API)

**Key class**: `OciStorageService extends AbstractStorageService`

**Credential resolution by AuthType**:
| AuthType | OCI Credential Provider |
|----------|------------------------|
| `ACCESS_KEY` | `SimpleAuthenticationDetailsProvider` (API key-based auth with tenancy/user/fingerprint/key) |
| `OIDC` | `OkeWorkloadIdentityAuthenticationDetailsProvider` (for OKE Kubernetes workloads) |
| `IAM` / `INSTANCE_PROFILE` | `InstancePrincipalsAuthenticationDetailsProvider` |

**Key differences from current S3-compatible approach**:
- Uses `ObjectStorageClient` instead of S3 client with endpoint override
- Native pre-authenticated request (PAR) API for signed URLs via `CreatePreauthenticatedRequestRequest`
- Uses `PutObjectRequest` with MD5 hash (matching current OCI behavior)
- Requires `namespace` parameter (OCI-specific) — added to `StorageConfig` as optional field or derived via `ObjectStorageClient.getNamespace()`

**HDFS prefix**: `oci://<bucket>@<namespace>/` (OCI native) or configurable

---

## 6. Authentication Architecture

### OIDC as Primary Auth

OIDC-based auth is the default/primary approach for cloud-native Kubernetes deployments:

| CSP | OIDC Mechanism | Environment Variables |
|-----|---------------|----------------------|
| AWS | STS AssumeRoleWithWebIdentity | `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`, `AWS_ROLE_SESSION_NAME` |
| Azure | Workload Identity Federation | `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE` |
| GCP | Workload Identity Federation | `GOOGLE_APPLICATION_CREDENTIALS` (credential config JSON) |
| OCI | OKE Workload Identity | OCI-specific service account token |

### Configuration Example

```java
// OIDC auth (new primary approach — no keys needed)
StorageConfig config = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.OIDC)
    .region("us-east-1")
    .build();

// Access Key auth (configurable fallback)
StorageConfig config = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.ACCESS_KEY)
    .storageKey("AKIA...")
    .storageSecret("secret...")
    .build();

// Factory creates the right service via ServiceLoader
IStorageService service = StorageServiceFactory.getStorageService(config);
```

---

## 7. Implementation Phases

### Phase 1: Project Structure + API Module
1. Create parent POM with multi-module structure
2. Remove all existing Scala source code and Scala-related build configuration
3. Create `cloud-storage-sdk-api` module POM
4. Implement `IStorageService.java` interface
5. Implement `Blob.java`, `DeleteTarget.java`, `StorageServiceException.java`
6. Implement `StorageConfig.java` with Builder pattern and enums
7. Implement `StorageServiceProvider.java` SPI interface
8. Implement `StorageServiceFactory.java` with ServiceLoader discovery
9. Implement `DateRangeUtil.java` (port from Joda-Time to java.time)
10. Implement `FileUtil.java` (port from CommonUtil.scala)
11. Implement `AbstractStorageService.java` with shared orchestration logic

### Phase 2: AWS Module
1. Create `cloud-storage-sdk-aws` module POM
2. Implement `AwsStorageService.java` with AWS SDK v2 S3Client / S3Presigner
3. Implement credential resolution (access key, OIDC via STS, IAM)
4. Handle Ceph S3 (endpoint override + path-style access)
5. Implement `AwsStorageServiceProvider.java` + META-INF/services registration
6. Write unit tests

### Phase 3: Azure Module
1. Create `cloud-storage-sdk-azure` module POM
2. Implement `AzureStorageService.java` with BlobServiceClient
3. Implement credential resolution (shared key, workload identity, managed identity)
4. Implement SAS token generation for signed URLs
5. Implement `AzureStorageServiceProvider.java` + META-INF/services registration
6. Write unit tests

### Phase 4: GCP Module
1. Create `cloud-storage-sdk-gcp` module POM
2. Implement `GcpStorageService.java` with native com.google.cloud.storage.Storage
3. Implement credential resolution (service account, WIF, application default)
4. Port V4 signed URL logic from current `GcloudStorageService.getPutSignedURL()`
5. Implement `GcpStorageServiceProvider.java` + META-INF/services registration
6. Write unit tests

### Phase 5: OCI Module
1. Create `cloud-storage-sdk-oci` module POM
2. Implement `OciStorageService.java` with OCI ObjectStorageClient
3. Implement credential resolution (API key, instance principal, OKE workload identity)
4. Handle OCI-specific concerns (namespace, MD5 on upload, pre-authenticated requests)
5. Implement `OciStorageServiceProvider.java` + META-INF/services registration
6. Write unit tests

### Phase 6: CI/CD and Release
1. Update `.github/workflows/maven-release.yml` for multi-module build
2. Configure individual module publishing to Maven Central
3. Update README with architecture overview and migration guide

---

## 8. Consumer Migration Guide

### Dependency Change

**Before:**
```xml
<dependency>
    <groupId>org.sunbird</groupId>
    <artifactId>cloud-store-sdk_2.13</artifactId>
    <version>1.4.10</version>
</dependency>
```

**After (e.g., for AWS):**
```xml
<dependency>
    <groupId>org.sunbird</groupId>
    <artifactId>cloud-storage-sdk-api</artifactId>
    <version>2.0.0</version>
</dependency>
<dependency>
    <groupId>org.sunbird</groupId>
    <artifactId>cloud-storage-sdk-aws</artifactId>
    <version>2.0.0</version>
    <scope>runtime</scope>
</dependency>
```

### Key API Changes

| Before (Scala) | After (Java) |
|----------------|-------------|
| `Option(false)` parameters | `boolean` params with `default` method overloads |
| `service.closeContext()` | `service.close()` (AutoCloseable / try-with-resources) |
| `StorageConfig("aws", key, secret)` | `StorageConfig.builder(AWS).storageKey(key).storageSecret(secret).build()` |
| `List[(String, Boolean)]` | `List<DeleteTarget>` |
| `searchObjectkeys` (lowercase k) | `searchObjectKeys` (camelCase) |
| `import o.s.c.s.factory.StorageServiceFactory` | `import o.s.c.s.StorageServiceFactory` (package changed) |

### Scala consumers
Since the new modules are pure Java JARs, Scala consumers can use them directly without Scala version cross-compilation issues:
```scala
import org.sunbird.cloud.storage._
val config = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.OIDC)
    .build()
val service = StorageServiceFactory.getStorageService(config)
```
