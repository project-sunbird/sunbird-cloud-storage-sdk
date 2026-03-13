# Sunbird Cloud Storage SDK — Technical Integration Guide

**Version:** 2.0.0 &nbsp;|&nbsp; **Java:** 11 &nbsp;|&nbsp; **Build:** Maven
**Frameworks:** Play Framework 2.x · Spring Boot 2.x / 3.x
**Supported CSPs:** Amazon Web Services (S3) · Microsoft Azure (Blob) · Google Cloud Storage · Oracle Cloud (OCI)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Maven Dependency Setup](#3-maven-dependency-setup)
   - [3.1 Spring Boot — pom.xml](#31-spring-boot--pomxml)
   - [3.2 Play Framework 2.x — build.sbt](#32-play-framework-2x--buildsbt)
   - [3.3 Transitive Dependencies per CSP](#33-transitive-dependencies-per-csp)
4. [Configuration](#4-configuration)
   - [4.1 StorageConfig Builder](#41-storageconfig-builder)
   - [4.2 AuthType Values](#42-authtype-values)
   - [4.3 Configuration Examples](#43-configuration-examples)
   - [4.4 Spring Boot — application.yml Integration](#44-spring-boot--applicationyml-integration)
   - [4.5 Play Framework — application.conf Integration](#45-play-framework--applicationconf-integration)
5. [Usage Examples](#5-usage-examples)
   - [5.1 Instantiation via ServiceLoader](#51-instantiation-via-serviceloader)
   - [5.2 Upload Operations](#52-upload-operations)
   - [5.3 Download Operations](#53-download-operations)
   - [5.4 Signed URLs](#54-signed-urls)
   - [5.5 List and Search Operations](#55-list-and-search-operations)
   - [5.6 Delete Operations](#56-delete-operations)
   - [5.7 Copy and Archive Operations](#57-copy-and-archive-operations)
6. [Dockerfile — Multi-Stage Builds per CSP](#6-dockerfile--multi-stage-builds-per-csp)
   - [6.1 AWS S3 Dockerfile](#61-aws-s3-dockerfile)
   - [6.2 Azure Blob Storage Dockerfile](#62-azure-blob-storage-dockerfile)
   - [6.3 Google Cloud Storage Dockerfile](#63-google-cloud-storage-dockerfile)
   - [6.4 Oracle Cloud (OCI) Dockerfile](#64-oracle-cloud-oci-dockerfile)
   - [6.5 Play Framework Dockerfile (Any CSP)](#65-play-framework-dockerfile-any-csp)
7. [Recommended Docker Image Strategy](#7-recommended-docker-image-strategy)
   - [7.1 Option A — Build-Time CSP Selection (Recommended)](#71-option-a--build-time-csp-selection-recommended)
   - [7.2 Option B — Single Image with All CSP Modules](#72-option-b--single-image-with-all-csp-modules)
   - [7.3 Comparison and Recommendation](#73-comparison-and-recommendation)
8. [Runtime Environment Variables](#8-runtime-environment-variables)
9. [Exception Handling](#9-exception-handling)
10. [Quick Reference](#10-quick-reference)

---

## 1. Overview

The Sunbird Cloud Storage SDK (v2.0.0) is a **multi-module Java 11 Maven SDK** providing a unified interface to major cloud object storage providers. It replaces the previous Scala/jclouds v1.x implementation with a clean, CSP-agnostic API backed by each provider's official SDK.

### Architecture

The SDK is organized as a Maven multi-module project with a strict separation between the API contract and provider implementations:

| Module | Purpose |
|--------|---------|
| `cloud-storage-sdk-api` | Interface, models, shared orchestration. **No CSP dependencies.** This is the only compile-time dependency your application needs. |
| `cloud-storage-sdk-aws` | AWS S3 + Ceph S3-compatible (AWS SDK v2). Include as `runtime` for AWS/Ceph deployments. |
| `cloud-storage-sdk-azure` | Azure Blob Storage (Azure SDK for Java). Include as `runtime` for Azure deployments. |
| `cloud-storage-sdk-gcp` | Google Cloud Storage (Google Cloud Java SDK). Include as `runtime` for GCP deployments. |
| `cloud-storage-sdk-oci` | OCI Object Storage (OCI Java SDK). Include as `runtime` for OCI deployments. |

The key design principle is that **your application code depends only on the API module at compile time**. The CSP implementation module is added as a `runtime` dependency and is auto-discovered via Java's `ServiceLoader` — no code changes are needed when switching providers.

> **OIDC-First Design:** `AuthType.OIDC` workload identity is the intended production authentication method. `ACCESS_KEY` is provided for local development and legacy environments only. In Kubernetes and cloud-native deployments, always prefer OIDC to avoid managing long-lived credentials.

---

## 2. Prerequisites

| Requirement | Details |
|-------------|---------|
| Java | 11 or later (LTS recommended: 11, 17, 21) |
| Maven | 3.6.3 or later |
| SDK Artifact Repository | `sunbird-cloud-storage-sdk:2.0.0` published to your Maven repository (Nexus, Artifactory, GitHub Packages, etc.) |
| Framework | Play Framework 2.x (Scala/Java) or Spring Boot 2.x / 3.x |
| Docker | 20.10+ for building container images (optional but recommended) |

---

## 3. Maven Dependency Setup

The SDK uses a **compile/runtime split** pattern: your application declares a `compile` dependency on the API module and a `runtime` dependency on exactly one CSP implementation module. This keeps CSP-specific code off your compile classpath and ensures that only the intended provider is active at runtime.

### 3.1 Spring Boot — pom.xml

```xml
<dependencies>

    <!-- ================================================================ -->
    <!-- COMPILE: API contract — your code imports only these interfaces  -->
    <!-- ================================================================ -->
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-api</artifactId>
        <version>2.0.0</version>
    </dependency>

    <!-- ================================================================ -->
    <!-- RUNTIME: Uncomment exactly ONE block for your target CSP         -->
    <!-- ================================================================ -->

    <!-- Option A: AWS S3 / Ceph S3-compatible -->
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-aws</artifactId>
        <version>2.0.0</version>
        <scope>runtime</scope>
    </dependency>

    <!-- Option B: Azure Blob Storage -->
    <!--
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-azure</artifactId>
        <version>2.0.0</version>
        <scope>runtime</scope>
    </dependency>
    -->

    <!-- Option C: Google Cloud Storage -->
    <!--
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-gcp</artifactId>
        <version>2.0.0</version>
        <scope>runtime</scope>
    </dependency>
    -->

    <!-- Option D: Oracle Cloud (OCI) Object Storage -->
    <!--
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-oci</artifactId>
        <version>2.0.0</version>
        <scope>runtime</scope>
    </dependency>
    -->

</dependencies>
```

### 3.2 Play Framework 2.x — build.sbt

For Play 2 projects using SBT, the `% "runtime"` suffix is equivalent to Maven's `runtime` scope:

```scala
libraryDependencies ++= Seq(

  // ── COMPILE: API contract ─────────────────────────────────────────────────
  "org.sunbird" % "cloud-storage-sdk-api" % "2.0.0",

  // ── RUNTIME: Uncomment exactly ONE provider ───────────────────────────────

  // Option A: AWS S3 / Ceph S3-compatible
  "org.sunbird" % "cloud-storage-sdk-aws" % "2.0.0" % "runtime",

  // Option B: Azure Blob Storage
  // "org.sunbird" % "cloud-storage-sdk-azure" % "2.0.0" % "runtime",

  // Option C: Google Cloud Storage
  // "org.sunbird" % "cloud-storage-sdk-gcp" % "2.0.0" % "runtime",

  // Option D: OCI Object Storage
  // "org.sunbird" % "cloud-storage-sdk-oci" % "2.0.0" % "runtime"
)
```

> **Why `runtime` scope?** The CSP module is loaded by Java's `ServiceLoader` at runtime. It must be on the classpath but must **not** be on the compile classpath — this prevents your application code from accidentally importing CSP-specific classes, keeping it portable across cloud providers.

### 3.3 Transitive Dependencies per CSP

Each runtime implementation module automatically pulls in the following dependencies. You do not need to declare these explicitly:

| CSP Module | Key Transitive Dependencies |
|------------|----------------------------|
| `cloud-storage-sdk-aws` | `software.amazon.awssdk:s3:2.25.0`, `software.amazon.awssdk:sts:2.25.0` |
| `cloud-storage-sdk-azure` | `com.azure:azure-storage-blob:12.25.0`, `com.azure:azure-identity:1.11.0` |
| `cloud-storage-sdk-gcp` | `com.google.cloud:google-cloud-storage:2.30.0` |
| `cloud-storage-sdk-oci` | `com.oracle.oci.sdk:oci-java-sdk-objectstorage:3.30.0`, `com.oracle.oci.sdk:oci-java-sdk-common-httpclient-jersey3:3.30.0` |

---

## 4. Configuration

All configuration is expressed through the `StorageConfig` builder. The builder requires a `StorageType` and optionally an `AuthType` plus additional parameters depending on your authentication strategy.

### 4.1 StorageConfig Builder

| Method | Description |
|--------|-------------|
| `.builder(StorageType)` | Starts the builder for the given CSP. **Required.** |
| `.authType(AuthType)` | Authentication method. Defaults to `ACCESS_KEY` if not set. |
| `.storageKey(String)` | Access key ID (`ACCESS_KEY`) or user OCID (OCI). |
| `.storageSecret(String)` | Secret access key, private key PEM (OCI), or service-account JSON (GCP). |
| `.region(String)` | Cloud region, e.g. `us-east-1` (AWS), `eastus` (Azure), `us-central1` (GCP), `us-ashburn-1` (OCI). |
| `.endPoint(String)` | Custom endpoint URL. **Required for Ceph S3** / S3-compatible stores. |
| `.build()` | Builds and returns an immutable `StorageConfig` instance. |

### 4.2 AuthType Values

| AuthType | Description | Supported CSPs |
|----------|-------------|---------------|
| `ACCESS_KEY` | Static credentials (key + secret). Use for local development and legacy environments. | All CSPs |
| `OIDC` | Workload Identity / Web Identity Token. **Preferred for Kubernetes deployments.** | All CSPs |
| `IAM` | Managed Identity / Application Default Credentials. For VMs and managed services. | AWS, Azure, GCP |
| `IAM_ROLE` | AWS STS assumed role. | AWS only |
| `INSTANCE_PROFILE` | EC2 instance profile or OCI Instance Principal. | AWS, OCI |

### 4.3 Configuration Examples

#### OIDC (Production — Kubernetes Workload Identity)

```java
// AWS — OIDC via Web Identity Token (EKS IRSA)
StorageConfig awsConfig = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.OIDC)
    .region("us-east-1")
    .build();

// Azure — Workload Identity (AKS)
// Env vars required: AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_FEDERATED_TOKEN_FILE
StorageConfig azureConfig = StorageConfig.builder(StorageConfig.StorageType.AZURE)
    .authType(StorageConfig.AuthType.OIDC)
    .storageKey("my-storage-account")   // account name
    .region("eastus")
    .build();

// GCP — Application Default Credentials (GKE Workload Identity)
StorageConfig gcpConfig = StorageConfig.builder(StorageConfig.StorageType.GCLOUD)
    .authType(StorageConfig.AuthType.OIDC)
    .region("us-central1")
    .build();

// OCI — Instance Principal (OKE)
StorageConfig ociConfig = StorageConfig.builder(StorageConfig.StorageType.OCI)
    .authType(StorageConfig.AuthType.INSTANCE_PROFILE)
    .region("us-ashburn-1")
    .build();
```

#### ACCESS_KEY (Development / Legacy)

```java
// AWS — Static credentials
StorageConfig awsConfig = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.ACCESS_KEY)
    .storageKey(System.getenv("AWS_ACCESS_KEY_ID"))
    .storageSecret(System.getenv("AWS_SECRET_ACCESS_KEY"))
    .region("us-east-1")
    .build();

// Ceph S3-compatible (uses AWS module with endpoint override)
StorageConfig cephConfig = StorageConfig.builder(StorageConfig.StorageType.CEPHS3)
    .authType(StorageConfig.AuthType.ACCESS_KEY)
    .endPoint("https://ceph-rgw.internal.example.com")
    .storageKey(System.getenv("CEPH_ACCESS_KEY"))
    .storageSecret(System.getenv("CEPH_SECRET_KEY"))
    .region("default")
    .build();

// Azure — Shared Key
StorageConfig azureConfig = StorageConfig.builder(StorageConfig.StorageType.AZURE)
    .authType(StorageConfig.AuthType.ACCESS_KEY)
    .storageKey("my-storage-account")                    // account name
    .storageSecret(System.getenv("AZURE_ACCOUNT_KEY"))   // account key
    .region("eastus")
    .build();

// GCP — Service Account JSON
StorageConfig gcpConfig = StorageConfig.builder(StorageConfig.StorageType.GCLOUD)
    .authType(StorageConfig.AuthType.ACCESS_KEY)
    .storageSecret(System.getenv("GOOGLE_SA_JSON"))  // full JSON string
    .build();

// OCI — API Key (ACCESS_KEY)
// Also requires OCI_TENANCY_ID and OCI_FINGERPRINT env vars
StorageConfig ociConfig = StorageConfig.builder(StorageConfig.StorageType.OCI)
    .authType(StorageConfig.AuthType.ACCESS_KEY)
    .storageKey(System.getenv("OCI_USER_OCID"))        // user OCID
    .storageSecret(System.getenv("OCI_PRIVATE_KEY_PEM")) // private key PEM
    .region("us-ashburn-1")
    .build();
```

### 4.4 Spring Boot — application.yml Integration

**application.yml:**

```yaml
sunbird:
  storage:
    type: AWS                        # AWS | AZURE | GCLOUD | OCI | CEPHS3
    auth-type: OIDC                  # OIDC | ACCESS_KEY | IAM | IAM_ROLE | INSTANCE_PROFILE
    region: us-east-1
    storage-key: ${STORAGE_KEY:}     # Optional — leave blank for OIDC/IAM
    storage-secret: ${STORAGE_SECRET:}
    endpoint: ${STORAGE_ENDPOINT:}   # Required only for Ceph S3
```

**StorageProperties.java:**

```java
@Configuration
@ConfigurationProperties(prefix = "sunbird.storage")
public class StorageProperties {
    private String type;
    private String authType;
    private String region;
    private String storageKey;
    private String storageSecret;
    private String endpoint;
    // getters + setters ...
}
```

**StorageServiceConfig.java:**

```java
@Configuration
public class StorageServiceConfig {

    @Bean
    @Singleton
    public IStorageService storageService(StorageProperties props) {
        StorageConfig.Builder builder = StorageConfig
            .builder(StorageConfig.StorageType.valueOf(props.getType()))
            .authType(StorageConfig.AuthType.valueOf(props.getAuthType()));

        if (props.getRegion() != null)        builder.region(props.getRegion());
        if (props.getStorageKey() != null)    builder.storageKey(props.getStorageKey());
        if (props.getStorageSecret() != null) builder.storageSecret(props.getStorageSecret());
        if (props.getEndpoint() != null)      builder.endPoint(props.getEndpoint());

        return StorageServiceFactory.getStorageService(builder.build());
    }
}
```

### 4.5 Play Framework — application.conf Integration

**application.conf:**

```hocon
sunbird.storage {
  type      = "AWS"
  auth-type = "OIDC"
  region    = "us-east-1"
  storage-key    = ${?STORAGE_KEY}
  storage-secret = ${?STORAGE_SECRET}
  endpoint       = ${?STORAGE_ENDPOINT}
}
```

**StorageModule.java (Guice Module):**

```java
public class StorageModule extends AbstractModule {

    private final Configuration config;

    public StorageModule(Environment env, Configuration config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        String type     = config.getString("sunbird.storage.type");
        String authType = config.getString("sunbird.storage.auth-type");
        String region   = config.getString("sunbird.storage.region");
        String key      = config.getString("sunbird.storage.storage-key");
        String secret   = config.getString("sunbird.storage.storage-secret");
        String endpoint = config.getString("sunbird.storage.endpoint");

        StorageConfig.Builder builder = StorageConfig
            .builder(StorageConfig.StorageType.valueOf(type))
            .authType(StorageConfig.AuthType.valueOf(authType));

        if (region != null)   builder.region(region);
        if (key != null)      builder.storageKey(key);
        if (secret != null)   builder.storageSecret(secret);
        if (endpoint != null) builder.endPoint(endpoint);

        IStorageService service = StorageServiceFactory.getStorageService(builder.build());
        bind(IStorageService.class).toInstance(service);
    }
}
```

Register the module in `application.conf`:

```hocon
play.modules.enabled += "modules.StorageModule"
```

---

## 5. Usage Examples

All examples use the `IStorageService` interface. The same code works regardless of which CSP module is on the runtime classpath.

### 5.1 Instantiation via ServiceLoader

```java
// Factory auto-discovers the runtime CSP module via ServiceLoader
try (IStorageService service = StorageServiceFactory.getStorageService(config)) {

    // All operations here...

}  // close() called automatically — shuts down the underlying SDK client

// Direct instantiation (bypasses ServiceLoader; only when one CSP is on classpath)
IStorageService service = new AwsStorageService(config);
```

> `IStorageService extends AutoCloseable` — always use try-with-resources to ensure the underlying SDK client is properly shut down.

### 5.2 Upload Operations

```java
// Upload a single file (with retry support)
String objectUrl = service.upload(
    "my-bucket",           // container / bucket name
    "/local/path/file.csv",// local file path
    "data/2026/file.csv",  // remote object key
    false,                 // not a directory
    1,                     // attempt number (start at 1)
    3,                     // max retries
    null                   // TTL in seconds (null = no expiry metadata)
);

// Convenience overload — no retries, no TTL
String objectUrl = service.upload("my-bucket", "/local/file.csv", "data/file.csv");

// Upload a directory recursively (async)
CompletableFuture<List<String>> future = service.uploadFolder(
    "my-bucket",
    "/local/export-folder",
    "exports/2026-02/",
    false,   // not public
    null,    // no TTL
    2,       // retries
    1        // attempt
);
List<String> uploadedKeys = future.get();  // block for result

// Upload raw bytes
byte[] jsonBytes = objectMapper.writeValueAsBytes(payload);
service.put("my-bucket", jsonBytes, "events/event-001.json");
```

### 5.3 Download Operations

```java
// Download a single object
service.download("my-bucket", "data/2026/file.csv", "/tmp/downloads/");

// Download a directory recursively
service.download("my-bucket", "exports/2026-02/", "/tmp/export/", true);

// Read object content as text lines
List<String> lines = service.getObjectData("my-bucket", "config/settings.json");

// Fetch blob metadata (without payload)
Blob blob = service.getObject("my-bucket", "data/2026/file.csv");
System.out.println("Size: " + blob.getContentLength());
System.out.println("Modified: " + blob.getLastModified());

// Fetch blob metadata with content bytes
Blob blobWithPayload = service.getObject("my-bucket", "small-file.txt", true);
String content = new String(blobWithPayload.getPayload(), StandardCharsets.UTF_8);
```

### 5.4 Signed URLs

```java
// Signed read URL — valid for 1 hour
String getUrl = service.getSignedURL(
    "my-bucket",
    "reports/annual-report.pdf",
    3600,    // TTL in seconds
    "r"      // permission: "r" = read, "w" = write
);

// Signed upload URL with content type (e.g. for direct browser upload)
String putUrl = service.getSignedURLV2(
    "my-bucket",
    "uploads/user-avatar.png",
    900,                               // 15-minute TTL
    "w",                               // write permission
    "image/png",                       // content type
    Map.of("x-amz-acl", "private")    // additional params (CSP-specific)
);
```

### 5.5 List and Search Operations

```java
// List all object keys under a prefix
List<String> keys = service.listObjectKeys("my-bucket", "data/2026/");

// List blobs with metadata
List<Blob> blobs = service.listObjects("my-bucket", "data/2026/");

// Date-based search — finds objects in date-prefixed paths (e.g. telemetry/raw/2026-02-14/)
List<Blob> recent = service.searchObjects(
    "my-bucket",
    "telemetry/raw/",
    null,            // fromDate — null means compute from toDate minus delta
    "2026-02-20",    // toDate
    7,               // delta: go back 7 days
    "yyyy-MM-dd"     // date pattern used in path segments
);

// Search returning only keys
List<String> recentKeys = service.searchObjectKeys(
    "my-bucket", "telemetry/raw/", null, "2026-02-20", 7, "yyyy-MM-dd"
);
```

### 5.6 Delete Operations

```java
// Delete a single file
service.deleteObject("my-bucket", "data/old-file.csv");

// Delete a directory and all its contents
service.deleteObject("my-bucket", "exports/old-export/", true);

// Delete multiple typed targets (mix of files and directories)
service.deleteObjects("my-bucket", List.of(
    DeleteTarget.file("data/file-a.csv"),
    DeleteTarget.file("data/file-b.csv"),
    DeleteTarget.directory("temp/staging/")
));
```

### 5.7 Copy and Archive Operations

```java
// Copy within the same bucket
service.copyObjects("my-bucket", "src/file.csv", "my-bucket", "dst/file.csv");

// Cross-bucket copy of an entire directory
service.copyObjects(
    "source-bucket", "archive/2025/",
    "dest-bucket",   "archive/2025/",
    true  // isDirectory
);

// Extract a ZIP archive from cloud storage to a new cloud prefix
// Downloads the ZIP, extracts locally, then uploads all files to the destination key
service.extractArchive(
    "my-bucket",
    "zips/dataset.zip",      // source ZIP object key
    "datasets/extracted/"    // destination prefix in the same bucket
);
```

---

## 6. Dockerfile — Multi-Stage Builds per CSP

All Dockerfiles below use a **multi-stage build**:

- **Stage 1 (`builder`):** Compiles the application using a full JDK + Maven/SBT image.
- **Stage 2 (runtime):** Contains only a JRE, the assembled JAR, and no build tools.

This pattern minimises final image size, reduces the CVE attack surface, and prevents build credentials or source code from reaching production.

### 6.1 AWS S3 Dockerfile

For AWS deployments, the SDK reads credentials from the pod's OIDC token (EKS IRSA) or instance profile automatically. No credentials are baked into the image.

```dockerfile
# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build

# Cache Maven dependencies before copying source (faster rebuilds)
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src

# Build fat-JAR; integration tests are skipped (they require real cloud access)
RUN mvn clean package -DskipTests -B


# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre-jammy

# Security: run as non-root
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY --from=builder /build/target/*.jar app.jar

USER appuser

# AWS SDK reads credentials from environment / IRSA token automatically.
# Supply these at runtime via your Kubernetes ServiceAccount annotation,
# ECS task role, or EC2 instance profile — do NOT hardcode them here.
#
# Required env vars (set via K8s secret or pod spec):
#   AWS_REGION                   — e.g. us-east-1
#   AWS_ROLE_ARN                 — for IRSA (EKS workload identity)
#   AWS_WEB_IDENTITY_TOKEN_FILE  — injected automatically by EKS

EXPOSE 9000

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

### 6.2 Azure Blob Storage Dockerfile

For AKS deployments, Azure Workload Identity injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and the federated token file automatically via the pod's service account. No credentials are baked into the image.

```dockerfile
# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests -B


# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre-jammy

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY --from=builder /build/target/*.jar app.jar

USER appuser

# Azure Workload Identity injects these automatically on AKS:
#   AZURE_CLIENT_ID
#   AZURE_TENANT_ID
#   AZURE_FEDERATED_TOKEN_FILE
#
# For ACCESS_KEY auth, inject AZURE_ACCOUNT_KEY via a K8s Secret.
#
# StorageConfig should be set with:
#   .authType(StorageConfig.AuthType.OIDC)
#   .storageKey("<storage-account-name>")

EXPOSE 9000

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

### 6.3 Google Cloud Storage Dockerfile

On GKE with Workload Identity, the GCP SDK reads credentials from the node metadata server automatically — no environment variables are needed when using `AuthType.OIDC` or `IAM`.

```dockerfile
# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests -B


# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre-jammy

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY --from=builder /build/target/*.jar app.jar

USER appuser

# GKE Workload Identity: no env vars required.
# Application Default Credentials (ADC) are automatically resolved via
# the GKE metadata server when the pod's KSA is annotated with a GSA.
#
# For service-account key auth (dev only), mount the JSON key file
# as a K8s Secret Volume and set:
#   GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa-key.json
# Never bake service account keys into the image.

EXPOSE 9000

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

### 6.4 Oracle Cloud (OCI) Dockerfile

OCI deployments on OKE use Instance Principals (`INSTANCE_PROFILE` AuthType). The OCI SDK communicates with the instance identity service automatically. `OCI_TENANCY_ID` and `OCI_FINGERPRINT` are only required for `ACCESS_KEY` auth.

```dockerfile
# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests -B


# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre-jammy

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY --from=builder /build/target/*.jar app.jar

USER appuser

# OKE Instance Principals: the OCI SDK automatically contacts the
# Instance Metadata Service (IMDS) on the node — no env vars needed.
#
# For ACCESS_KEY auth only (supply via K8s Secret):
#   OCI_TENANCY_ID   — tenancy OCID
#   OCI_FINGERPRINT  — API key fingerprint
#   (storageKey and storageSecret come from StorageConfig)

EXPOSE 9000

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

### 6.5 Play Framework Dockerfile (Any CSP)

Play 2 applications are packaged as a distribution directory (`sbt dist`) rather than a single fat JAR. The multi-stage build uses SBT to produce the `dist` package, then copies only the unpacked distribution into the JRE image.

```dockerfile
# ── Stage 1: Build with SBT ────────────────────────────────────────────────────
FROM sbtscala/scala-sbt:eclipse-temurin-11.0.21_9_1.9.8_2.13.12 AS builder

WORKDIR /build

# Cache SBT dependencies before copying source
COPY build.sbt .
COPY project ./project
RUN sbt update

COPY . .

# Build universal distribution ZIP
RUN sbt clean dist

# Unzip the distribution
RUN cd target/universal \
    && unzip *.zip \
    && mv $(ls -d */ | head -1) /app-dist


# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre-jammy

RUN groupadd -r appuser && useradd -r -g appuser appuser

COPY --from=builder /app-dist /app
RUN chmod +x /app/bin/*

USER appuser

# Play application port
EXPOSE 9000

# APPLICATION_SECRET must be supplied at runtime via a K8s Secret.
# Never hardcode it here.
#   -DAPPLICATION_SECRET=$(cat /run/secrets/play-secret)
ENTRYPOINT ["/app/bin/my-play-app", "-Dhttp.port=9000"]
```

---

## 7. Recommended Docker Image Strategy

When containerising applications that use the Sunbird Cloud Storage SDK, there are two common approaches.

### 7.1 Option A — Build-Time CSP Selection (Recommended)

Use a Maven profile and Docker `--build-arg` to select the CSP at image build time. Each CI pipeline job produces a tagged image for its target cloud.

**Dockerfile.dynamic:**

```dockerfile
ARG CSP=aws

# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-11 AS builder
ARG CSP

WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B -P${CSP}

COPY src ./src
RUN mvn clean package -DskipTests -B -P${CSP}

# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre-jammy
ARG CSP

RUN groupadd -r appuser && useradd -r -g appuser appuser
WORKDIR /app
COPY --from=builder /build/target/*.jar app.jar

LABEL org.sunbird.csp=${CSP}

USER appuser
EXPOSE 9000
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

**Build commands:**

```bash
docker build --build-arg CSP=aws   -t myapp:2.0-aws   .
docker build --build-arg CSP=azure -t myapp:2.0-azure .
docker build --build-arg CSP=gcp   -t myapp:2.0-gcp   .
docker build --build-arg CSP=oci   -t myapp:2.0-oci   .
```

**Maven profiles in pom.xml:**

```xml
<profiles>
    <profile>
        <id>aws</id>
        <activation><activeByDefault>true</activeByDefault></activation>
        <dependencies>
            <dependency>
                <groupId>org.sunbird</groupId>
                <artifactId>cloud-storage-sdk-aws</artifactId>
                <version>2.0.0</version>
                <scope>runtime</scope>
            </dependency>
        </dependencies>
    </profile>
    <profile>
        <id>azure</id>
        <dependencies>
            <dependency>
                <groupId>org.sunbird</groupId>
                <artifactId>cloud-storage-sdk-azure</artifactId>
                <version>2.0.0</version>
                <scope>runtime</scope>
            </dependency>
        </dependencies>
    </profile>
    <profile>
        <id>gcp</id>
        <dependencies>
            <dependency>
                <groupId>org.sunbird</groupId>
                <artifactId>cloud-storage-sdk-gcp</artifactId>
                <version>2.0.0</version>
                <scope>runtime</scope>
            </dependency>
        </dependencies>
    </profile>
    <profile>
        <id>oci</id>
        <dependencies>
            <dependency>
                <groupId>org.sunbird</groupId>
                <artifactId>cloud-storage-sdk-oci</artifactId>
                <version>2.0.0</version>
                <scope>runtime</scope>
            </dependency>
        </dependencies>
    </profile>
</profiles>
```

### 7.2 Option B — Single Image with All CSP Modules

Include all four CSP modules in the image. The active provider is selected at runtime via `StorageConfig`. This maximises deployment flexibility but increases image size due to multiple large SDK JARs (~300–400 MB additional).

```xml
<!-- pom.xml — all four CSP modules as runtime -->
<dependencies>
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
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-azure</artifactId>
        <version>2.0.0</version>
        <scope>runtime</scope>
    </dependency>
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-gcp</artifactId>
        <version>2.0.0</version>
        <scope>runtime</scope>
    </dependency>
    <dependency>
        <groupId>org.sunbird</groupId>
        <artifactId>cloud-storage-sdk-oci</artifactId>
        <version>2.0.0</version>
        <scope>runtime</scope>
    </dependency>
</dependencies>
```

### 7.3 Comparison and Recommendation

| Criterion | Option A (CSP-tagged images) | Option B (All CSPs in one image) |
|-----------|------------------------------|----------------------------------|
| **Image size** | Lean — only the required CSP JARs are included | Larger — carries 4× the SDK JARs (~300–400 MB extra) |
| **Security surface** | Minimal — unused CSP SDKs and their CVEs are excluded | Broader — unused dependencies still present |
| **Deployment clarity** | Explicit CSP in image tag (`myapp:2.0-aws`) reduces misconfiguration | CSP determined at runtime via env var |
| **Multi-cloud flexibility** | Requires a rebuild to switch CSP | Switch CSP by changing an env var without a rebuild |
| **CI complexity** | Separate build job per CSP | Single build job |
| **Recommended for** | Single-cloud deployments (most enterprise use cases) | Multi-tenant SaaS where tenants use different CSPs |

> **Recommendation:** Use **Option A** (Maven profiles + CSP-tagged images) for the vast majority of deployments. Option B is only warranted when your application must serve multiple CSPs from the same running instance — for example, a multi-tenant platform where each tenant brings their own cloud account. For all single-cloud deployments, Option A produces smaller, more auditable, and more secure images.

---

## 8. Runtime Environment Variables

Set these via Kubernetes Secrets, pod annotations, or ECS task definitions. **Never hardcode credentials in Dockerfiles or configuration files.**

### AWS

| Variable | Description | When Required |
|----------|-------------|--------------|
| `AWS_REGION` | Cloud region, e.g. `us-east-1`. Also accepted as `AWS_DEFAULT_REGION`. | Always |
| `AWS_ACCESS_KEY_ID` | Static access key ID. | `ACCESS_KEY` only |
| `AWS_SECRET_ACCESS_KEY` | Static secret access key. | `ACCESS_KEY` only |
| `AWS_ROLE_ARN` | IAM role ARN for EKS IRSA. | `OIDC` (EKS) |
| `AWS_WEB_IDENTITY_TOKEN_FILE` | Path to OIDC token file. Injected automatically by EKS. | `OIDC` (EKS) |

### Azure

| Variable | Description | When Required |
|----------|-------------|--------------|
| `AZURE_CLIENT_ID` | Managed identity or app registration client ID. | `OIDC` / `IAM` |
| `AZURE_TENANT_ID` | Azure Active Directory tenant ID. | `OIDC` / `IAM` |
| `AZURE_FEDERATED_TOKEN_FILE` | Path to K8s service account token. Injected by AKS Workload Identity. | `OIDC` (AKS) |
| `AZURE_ACCOUNT_KEY` | Storage account access key. Read this into `storageSecret` in your `StorageModule`. | `ACCESS_KEY` only |

### GCP

| Variable | Description | When Required |
|----------|-------------|--------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to a service account JSON key file. For dev/legacy use only. | `ACCESS_KEY` only |
| *(none for GKE)* | GKE Workload Identity binds a KSA to a GSA via pod annotation. No env vars required. | `OIDC` (GKE) |

### OCI

| Variable | Description | When Required |
|----------|-------------|--------------|
| `OCI_TENANCY_ID` | Tenancy OCID. | `ACCESS_KEY` only |
| `OCI_FINGERPRINT` | API key fingerprint. | `ACCESS_KEY` only |
| *(none for OKE)* | OKE Instance Principal auth uses the node's IMDS endpoint automatically. | `INSTANCE_PROFILE` |

---

## 9. Exception Handling

All `IStorageService` methods throw `StorageServiceException`, an unchecked `RuntimeException`. Wrap calls appropriately in your service layer to distinguish transient from permanent failures.

```java
import org.sunbird.cloud.storage.exception.StorageServiceException;

@Service
public class ContentStorageService {

    private final IStorageService storage;

    public ContentStorageService(IStorageService storage) {
        this.storage = storage;
    }

    public String uploadContent(String bucket, Path localFile, String remoteKey) {
        try {
            return storage.upload(
                bucket,
                localFile.toString(),
                remoteKey,
                false,  // not a directory
                1,      // first attempt
                3,      // retry up to 3 times total
                null    // no TTL
            );
        } catch (StorageServiceException e) {
            log.error("Upload failed for key={}", remoteKey, e);
            throw new ContentServiceException("Storage upload failed", e);
        }
    }
}
```

> **Built-in Retry:** `AbstractStorageService` provides automatic retry orchestration for `upload()` and `put()` operations. The `attempt` parameter tracks the current attempt (start at `1`) and `retryCount` sets the maximum. You do not need to implement your own retry loop.

---

## 10. Quick Reference

### CSP Comparison

| CSP | Module Artifact | SDK Version | Recommended AuthType | HDFS Prefix Format |
|-----|----------------|-------------|---------------------|--------------------|
| AWS S3 | `cloud-storage-sdk-aws` | AWS SDK v2 `2.25.0` | `OIDC` (IRSA) | `s3n://<bucket>/` |
| Azure Blob | `cloud-storage-sdk-azure` | Azure SDK `12.25.0` | `OIDC` (Workload Identity) | `wasb://<container>@<account>.blob.core.windows.net/` |
| GCP Storage | `cloud-storage-sdk-gcp` | Google Cloud Java `2.30.0` | `OIDC` (GKE WI) | `gs://<bucket>/` |
| OCI Object Storage | `cloud-storage-sdk-oci` | OCI Java SDK `3.30.0` | `INSTANCE_PROFILE` | `oci://<bucket>@<namespace>/` |
| Ceph S3 | `cloud-storage-sdk-aws` | AWS SDK v2 `2.25.0` (via `endpointOverride`) | `ACCESS_KEY` | `s3n://<bucket>/` |

### Key Dependency Versions

| Component | Version |
|-----------|---------|
| SDK | `2.0.0` |
| Java target | `11` |
| AWS SDK v2 | `2.25.0` |
| Azure Blob Storage | `12.25.0` |
| Azure Identity | `1.11.0` |
| GCP Cloud Storage | `2.30.0` |
| OCI Java SDK | `3.30.0` |
| Apache Tika | `2.9.0` |
| JUnit 5 | `5.10.1` |
| Mockito | `5.8.0` |

### Build Commands

```bash
# Full build with unit tests (integration tests auto-skip without env vars)
mvn clean verify

# Build without tests
mvn clean package -DskipTests

# Build with specific CSP profile
mvn clean package -DskipTests -Paws
mvn clean package -DskipTests -Pazure
mvn clean package -DskipTests -Pgcp
mvn clean package -DskipTests -Poci

# Build a single module
mvn -pl cloud-storage-sdk-aws package -DskipTests

# Run unit tests only (no cloud access needed)
mvn -pl cloud-storage-sdk-api test

# Docker — build one image per CSP (Option A)
docker build --build-arg CSP=aws   -t myapp:2.0-aws   .
docker build --build-arg CSP=azure -t myapp:2.0-azure .
docker build --build-arg CSP=gcp   -t myapp:2.0-gcp   .
docker build --build-arg CSP=oci   -t myapp:2.0-oci   .
```

---

*Sunbird Cloud Storage SDK v2.0.0 — Generated February 2026*
