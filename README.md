# Sunbird Cloud Storage SDK

Multi-module Java 11 SDK providing cloud storage operations (upload, download, signed URLs, delete, copy, search, etc.) with CSP-specific implementations using official cloud SDKs.

## Modules

| Module | Artifact ID | Description |
|--------|-------------|-------------|
| API | `cloud-storage-sdk-api` | Interface, models, factory, and shared abstractions |
| AWS | `cloud-storage-sdk-aws` | AWS S3 + Ceph S3-compatible (AWS SDK v2) |
| Azure | `cloud-storage-sdk-azure` | Azure Blob Storage (Azure SDK for Java) |
| GCP | `cloud-storage-sdk-gcp` | Google Cloud Storage (Google Cloud Java SDK) |
| OCI | `cloud-storage-sdk-oci` | Oracle Cloud Object Storage (OCI Java SDK) |

## Authentication

OIDC-based authentication is the primary approach for cloud-native deployments. Access Key authentication is available as a configurable fallback.

| Auth Type | Description |
|-----------|-------------|
| `OIDC` | Workload Identity Federation / Web Identity Token (primary) |
| `ACCESS_KEY` | Static access key and secret |
| `IAM` | IAM role / managed identity |
| `INSTANCE_PROFILE` | Cloud instance metadata credentials |

## Usage

### Maven Dependency

Add the API module and one CSP-specific module:

```xml
<dependency>
    <groupId>org.sunbird</groupId>
    <artifactId>cloud-storage-sdk-api</artifactId>
    <version>2.0.0</version>
</dependency>

<!-- Pick one CSP module -->
<dependency>
    <groupId>org.sunbird</groupId>
    <artifactId>cloud-storage-sdk-aws</artifactId>
    <version>2.0.0</version>
    <scope>runtime</scope>
</dependency>
```

### Java Example

```java
import org.sunbird.cloud.storage.*;

// OIDC auth (cloud-native, no keys needed)
StorageConfig config = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.OIDC)
    .region("us-east-1")
    .build();

// Access Key auth (fallback)
StorageConfig config = StorageConfig.builder(StorageConfig.StorageType.AWS)
    .authType(StorageConfig.AuthType.ACCESS_KEY)
    .storageKey("AKIA...")
    .storageSecret("secret...")
    .build();

// Factory auto-discovers the CSP module via ServiceLoader
try (IStorageService service = StorageServiceFactory.getStorageService(config)) {
    // Upload
    String url = service.upload("my-bucket", "/local/file.txt", "path/file.txt");

    // Signed URL
    String signedUrl = service.getSignedURL("my-bucket", "path/file.txt", 3600, "r");

    // Download
    service.download("my-bucket", "path/file.txt", "/local/download/");

    // List
    List<String> keys = service.listObjectKeys("my-bucket", "path/");
}
```

## Building

```bash
mvn clean install
```

## Migration from v1.x

See [PLAN.md](PLAN.md) for the detailed migration guide from the Scala/jclouds-based v1.x.

## License

[MIT License](LICENSE)
