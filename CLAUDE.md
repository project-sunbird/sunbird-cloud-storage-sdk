# CLAUDE.md — Sunbird Cloud Storage SDK

This file gives Claude agents the essential context needed to work on this
repository without reading every source file first.

---

## What this repository is

A **multi-module Java 11 Maven SDK** for cloud object storage. It replaced a
single-module Scala/jclouds implementation (v1.x) with clean Java 11 modules
that use each CSP's official SDK and support OIDC-based workload identity as
the primary authentication approach.

**Current version: 2.0.0**

---

## Module layout

```
sunbird-cloud-storage-sdk/          ← parent POM (packaging=pom)
├── cloud-storage-sdk-api/          ← interface, models, shared abstractions (no CSP deps)
├── cloud-storage-sdk-aws/          ← AWS S3 + Ceph S3-compatible (AWS SDK v2)
├── cloud-storage-sdk-azure/        ← Azure Blob Storage (Azure SDK for Java)
├── cloud-storage-sdk-gcp/          ← Google Cloud Storage (Google Cloud Java SDK)
└── cloud-storage-sdk-oci/          ← OCI Object Storage (OCI Java SDK)
```

All CSP modules depend on `cloud-storage-sdk-api`. There are no
cross-dependencies between CSP modules.

---

## Key source files

### API module (`cloud-storage-sdk-api`)

| File | Purpose |
|---|---|
| `IStorageService.java` | Main public interface (19 methods + default overloads), extends `AutoCloseable` |
| `StorageConfig.java` | Builder-pattern config; holds `AuthType` and `StorageType` enums |
| `AbstractStorageService.java` | 12 abstract primitives + all orchestration (retry, folder upload, date search, archive extract) |
| `StorageServiceFactory.java` | `ServiceLoader`-based factory: `getStorageService(StorageConfig)` |
| `StorageServiceProvider.java` | SPI interface each CSP implements |
| `model/Blob.java` | Immutable blob metadata (key, contentLength, lastModified, metadata, payload) |
| `model/DeleteTarget.java` | Typed delete target; static factories `DeleteTarget.file(key)` / `DeleteTarget.directory(key)` |
| `util/DateRangeUtil.java` | Pure `java.time` date helpers: `getStartDate`, `getDatesBetween` |
| `util/FileUtil.java` | `listFilesRecursive`, `copyStream`, `unZip` |
| `exception/StorageServiceException.java` | Unchecked runtime exception wrapping all SDK errors |

### CSP modules

Each CSP module contains exactly two production classes:

```
<csp>/src/main/java/org/sunbird/cloud/storage/service/<csp>/
    <Csp>StorageService.java         ← extends AbstractStorageService
    <Csp>StorageServiceProvider.java ← implements StorageServiceProvider

<csp>/src/main/resources/META-INF/services/
    org.sunbird.cloud.storage.StorageServiceProvider   ← SPI registration
```

### HDFS prefix per CSP

| CSP | Prefix format |
|---|---|
| AWS | `s3n://<bucket>/` |
| Azure | `wasb://<container>@<account>.blob.core.windows.net/` |
| GCP | `gs://<bucket>/` |
| OCI | `oci://<bucket>@<namespace>/` |

---

## StorageConfig

```java
StorageConfig config = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.OIDC)
    .region("us-east-1")
    .build();
```

### AuthType enum

| Value | Meaning |
|---|---|
| `ACCESS_KEY` | storageKey + storageSecret (static credentials) |
| `OIDC` | Workload Identity / Web Identity Token |
| `IAM` | Managed Identity / default application credentials |
| `IAM_ROLE` | IAM role (AWS STS assumed role) |
| `INSTANCE_PROFILE` | EC2 instance profile / OCI Instance Principal |

### StorageType enum

`AWS`, `AZURE`, `GCLOUD`, `OCI`, `CEPHS3`

`CEPHS3` is handled by the AWS module with `endpointOverride` +
`pathStyleAccessEnabled(true)`. Set `endPoint` in `StorageConfig`.

---

## AbstractStorageService — the 12 primitives

CSP subclasses must implement these and nothing else:

```java
void ensureContainerExists(String container)
String putObject(String container, String objectKey, File file)
String putObject(String container, String objectKey, byte[] content)
InputStream getObjectStream(String container, String objectKey)
String getObjectUri(String container, String objectKey)
BlobDetail getObjectDetail(String container, String objectKey)  // inner class
List<String> listKeys(String container, String prefix)
void removeObjects(String container, List<String> objectKeys)
void copyObject(String fromContainer, String fromKey, String toContainer, String toKey)
String generateSignedGetUrl(String container, String objectKey, int ttlSeconds)
String generateSignedPutUrl(String container, String objectKey, int ttlSeconds,
                            String contentType, Map<String,String> additionalParams)
String getHdfsPrefix(String container)
```

The orchestration in `AbstractStorageService` (retry loops, `uploadFolder`,
`searchObjects` date iteration, `extractArchive`, directory copy) is shared
and must NOT be duplicated in CSP implementations.

---

## Authentication per CSP

### AWS (`AwsStorageService`)
- `ACCESS_KEY` → `StaticCredentialsProvider(AwsBasicCredentials)`
- `OIDC/IAM/IAM_ROLE/INSTANCE_PROFILE` → `DefaultCredentialsProvider`
- Custom endpoint → `S3ClientBuilder.endpointOverride(URI)` + `pathStyleAccessEnabled(true)`

### Azure (`AzureStorageService`)
- `ACCESS_KEY` → `StorageSharedKeyCredential(accountName, accountKey)`; `storageKey` = account name, `storageSecret` = account key
- `OIDC` → `WorkloadIdentityCredentialBuilder` (reads `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`) or `DefaultAzureCredentialBuilder`
- `IAM/INSTANCE_PROFILE` → `ManagedIdentityCredentialBuilder`
- Signed URLs: shared-key auth uses `generateSas()`; OIDC/IAM uses `getUserDelegationKey()` + `generateUserDelegationSas()`

### GCP (`GcpStorageService`)
- `ACCESS_KEY` → `GoogleCredentials.fromStream(new ByteArrayInputStream(secret.getBytes()))` where `storageSecret` is the service account JSON string
- `OIDC/IAM` → `GoogleCredentials.getApplicationDefault()`
- Signed URLs use V4 signatures; PUT URL supports chunked upload via `HttpMethod.POST` + `x-goog-resumable: start`

### OCI (`OciStorageService`)
- `ACCESS_KEY` → `SimpleAuthenticationDetailsProvider`; `storageKey` = user OCID, `storageSecret` = private key PEM; reads `OCI_TENANCY_ID` and `OCI_FINGERPRINT` from env
- `OIDC/IAM/INSTANCE_PROFILE` → `InstancePrincipalsAuthenticationDetailsProvider`
- OCI namespace is fetched via `getNamespace()` in the constructor and stored
- Signed URLs use OCI Pre-Authenticated Requests (PAR)
- All uploads compute and set `contentMD5` (base64-encoded MD5)

---

## Service discovery (ServiceLoader)

```java
// From user code — auto-discovers the right CSP:
IStorageService svc = StorageServiceFactory.getStorageService(config);

// Or directly instantiate a CSP class:
IStorageService svc = new AwsStorageService(config);
```

The `META-INF/services/org.sunbird.cloud.storage.StorageServiceProvider` file
in each CSP module registers the provider. When all CSP jars are on the
classpath, `StorageServiceFactory` selects the one whose
`provider.supports(config.getType())` returns `true`.

---

## Tests

### Unit tests (always run, no cloud access needed)

Located in `cloud-storage-sdk-api/src/test/`:

| Class | What it tests |
|---|---|
| `DateRangeUtilTest` | Date arithmetic helpers |
| `FileUtilTest` | File utilities with `@TempDir` |
| `AbstractStorageServiceTest` | All orchestration logic via `InMemoryStorageService` inner class |

### Integration tests (skipped by default)

Each CSP module has one integration test class that extends
`BaseStorageServiceIntegrationTest` (published as a test-jar from the API
module). Tests are guarded by `assumeTrue()` and only execute when the
`*_STORAGE_TEST_ENABLED` env var is set.

| Class | Enable with |
|---|---|
| `AwsStorageServiceTest` | `AWS_STORAGE_TEST_ENABLED=true` |
| `AzureStorageServiceTest` | `AZURE_STORAGE_TEST_ENABLED=true` |
| `GcpStorageServiceTest` | `GCP_STORAGE_TEST_ENABLED=true` |
| `OciStorageServiceTest` | `OCI_STORAGE_TEST_ENABLED=true` |

See **TESTING.md** for the full list of environment variables and usage examples.

---

## Build

```bash
# Full build, skip tests
mvn clean package -DskipTests

# Full build with unit tests (integration tests auto-skip)
mvn clean verify

# Single module
mvn -pl cloud-storage-sdk-aws package -DskipTests
```

### Maven build order (reactor)

1. `cloud-storage-sdk-parent` (parent POM)
2. `cloud-storage-sdk-api`
3. `cloud-storage-sdk-aws`, `cloud-storage-sdk-azure`, `cloud-storage-sdk-gcp`, `cloud-storage-sdk-oci` (parallel)

### Key versions (managed in parent POM)

| Dependency | Version |
|---|---|
| AWS SDK v2 | 2.25.0 |
| Azure Blob | 12.25.0 |
| Azure Identity | 1.11.0 |
| GCP Storage | 2.30.0 |
| OCI SDK | 3.30.0 |
| Apache Tika | 2.9.0 |
| JUnit Jupiter | 5.10.1 |
| Mockito | 5.8.0 |
| Java target | 11 |

---

## Package structure

```
org.sunbird.cloud.storage
├── IStorageService              (interface)
├── AbstractStorageService       (abstract class)
├── StorageConfig                (config + AuthType/StorageType enums)
├── StorageServiceFactory        (factory via ServiceLoader)
├── StorageServiceProvider       (SPI interface)
├── BaseStorageServiceIntegrationTest  (abstract integration test base, test-jar)
├── exception/
│   └── StorageServiceException
├── model/
│   ├── Blob
│   └── DeleteTarget
├── util/
│   ├── DateRangeUtil
│   └── FileUtil
└── service/
    ├── aws/   AwsStorageService, AwsStorageServiceProvider
    ├── azure/ AzureStorageService, AzureStorageServiceProvider
    ├── gcp/   GcpStorageService, GcpStorageServiceProvider
    └── oci/   OciStorageService, OciStorageServiceProvider
```

---

## Common patterns when modifying this codebase

### Adding a new operation to the interface
1. Add the method signature to `IStorageService.java` (add a `default` overload
   if optional params are involved)
2. If the operation needs a new primitive, add the `abstract` method to
   `AbstractStorageService.java` and implement it in all four CSP services
3. If orchestration logic is shared, implement it in `AbstractStorageService`
   and call the abstract primitive
4. Add unit tests in `AbstractStorageServiceTest` using `InMemoryStorageService`
5. Add integration test coverage in `BaseStorageServiceIntegrationTest`

### Adding a new CSP
1. Create a new Maven module `cloud-storage-sdk-<csp>/`
2. Add it to the parent POM `<modules>` list
3. Depend on `cloud-storage-sdk-api`
4. Extend `AbstractStorageService`, implement all 12 primitives
5. Implement `StorageServiceProvider`, register in `META-INF/services/`
6. Add `StorageType.<CSP>` to the enum in `StorageConfig.java`
7. Write an integration test extending `BaseStorageServiceIntegrationTest`

### Changing authentication logic
Each CSP's `resolveAuth(StorageConfig)` / auth setup is self-contained in the
CSP's service constructor. Do not share auth logic across CSPs.

---

## Important design decisions

- **No Scala, no jclouds** — complete clean break at v2.0.0
- **OIDC first** — `AuthType.OIDC` is the intended production auth; `ACCESS_KEY`
  is for local development and legacy environments
- **`Ceph S3` is handled in the AWS module** via `endpointOverride` (not a
  separate module)
- **`IStorageService extends AutoCloseable`** — use try-with-resources; `close()`
  shuts down the underlying SDK client
- **`DeleteTarget` replaces `(String, Boolean)` tuples** — use
  `DeleteTarget.file(key)` or `DeleteTarget.directory(key)`
- **Thread-safety** — retry counters use local variables, not instance fields
- **Test-jar** — `cloud-storage-sdk-api` publishes a `-tests` classifier jar so
  CSP modules can extend `BaseStorageServiceIntegrationTest` without duplication
