package org.sunbird.cloud.storage.service.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cloud.storage.AbstractStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.exception.StorageServiceException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Google Cloud Storage implementation using the Google Cloud Java SDK.
 */
public class GcpStorageService extends AbstractStorageService {

    private static final Logger logger = LoggerFactory.getLogger(GcpStorageService.class);
    private static final String GCS_HOST = "https://storage.googleapis.com/";

    private final StorageConfig config;
    private final Storage storage;

    public GcpStorageService(StorageConfig config) {
        this.config = config;
        this.storage = buildStorage(config);
        logger.info("Initialized GcpStorageService with authType={}", config.getAuthType());
    }

    private Storage buildStorage(StorageConfig config) {
        try {
            StorageOptions.Builder builder = StorageOptions.newBuilder();

            switch (config.getAuthType()) {
                case ACCESS_KEY:
                    // For access key auth, storageKey is the project ID and storageSecret
                    // is a service account JSON key. Parse as JSON credentials.
                    if (config.getStorageSecret() != null && !config.getStorageSecret().isEmpty()) {
                        GoogleCredentials credentials = GoogleCredentials.fromStream(
                                new ByteArrayInputStream(config.getStorageSecret().getBytes()));
                        builder.setCredentials(credentials);
                    }
                    if (config.getStorageKey() != null && !config.getStorageKey().isEmpty()) {
                        builder.setProjectId(config.getStorageKey());
                    }
                    break;
                case OIDC:
                case IAM:
                case IAM_ROLE:
                case INSTANCE_PROFILE:
                    // Use application default credentials (handles WIF, GKE Workload Identity,
                    // metadata server, GOOGLE_APPLICATION_CREDENTIALS)
                    builder.setCredentials(GoogleCredentials.getApplicationDefault());
                    break;
                default:
                    throw new StorageServiceException("Unsupported auth type: " + config.getAuthType());
            }

            return builder.build().getService();
        } catch (IOException e) {
            throw new StorageServiceException("Failed to initialize GCP Storage: " + e.getMessage(), e);
        }
    }

    @Override
    protected void ensureContainerExists(String container) {
        try {
            if (storage.get(container) == null) {
                storage.create(com.google.cloud.storage.BucketInfo.of(container));
                logger.info("Created bucket: {}", container);
            }
        } catch (Exception e) {
            // Bucket may already exist; log and continue
            logger.debug("Bucket check/create for {}: {}", container, e.getMessage());
        }
    }

    @Override
    protected String putObject(String container, String objectKey, File file) {
        try {
            String contentType = tika.detect(file);
            BlobId blobId = BlobId.of(container, objectKey);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                    .setContentType(contentType)
                    .build();
            storage.create(blobInfo, Files.readAllBytes(file.toPath()));
            return GCS_HOST + container + "/" + objectKey;
        } catch (IOException e) {
            throw new StorageServiceException(
                    "Failed to put object from file: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String putObject(String container, String objectKey, byte[] content) {
        BlobId blobId = BlobId.of(container, objectKey);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, content);
        return GCS_HOST + container + "/" + objectKey;
    }

    @Override
    protected InputStream getObjectStream(String container, String objectKey) {
        try {
            byte[] content = storage.readAllBytes(BlobId.of(container, objectKey));
            return new ByteArrayInputStream(content);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object stream: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String getObjectUri(String container, String objectKey) {
        return GCS_HOST + container + "/" + objectKey;
    }

    @Override
    protected BlobDetail getObjectDetail(String container, String objectKey) {
        try {
            com.google.cloud.storage.Blob blob = storage.get(BlobId.of(container, objectKey));
            if (blob == null) {
                throw new StorageServiceException("Object not found: " + container + "/" + objectKey);
            }

            Map<String, Object> metadata = new HashMap<>();
            if (blob.getMetadata() != null) {
                metadata.putAll(blob.getMetadata());
            }
            metadata.put("uri", GCS_HOST + container + "/" + objectKey);
            metadata.put("publicUri", GCS_HOST + container + "/" + objectKey);
            metadata.put("name", blob.getName());
            metadata.put("Content-Type", blob.getContentType());

            Long size = blob.getSize();
            long contentLength = size != null ? size : 0L;
            Date lastModified = blob.getUpdateTimeOffsetDateTime() != null
                    ? Date.from(blob.getUpdateTimeOffsetDateTime().toInstant())
                    : null;

            return new BlobDetail(objectKey, contentLength, lastModified, metadata,
                    GCS_HOST + container + "/" + objectKey);
        } catch (StorageServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object detail: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected List<String> listKeys(String container, String prefix) {
        try {
            List<String> keys = new ArrayList<>();
            com.google.api.gax.paging.Page<com.google.cloud.storage.Blob> blobs =
                    storage.list(container, Storage.BlobListOption.prefix(prefix));
            for (com.google.cloud.storage.Blob blob : blobs.iterateAll()) {
                keys.add(blob.getName());
            }
            return keys;
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to list keys with prefix: " + prefix + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected void removeObjects(String container, List<String> objectKeys) {
        try {
            List<BlobId> blobIds = new ArrayList<>();
            for (String key : objectKeys) {
                blobIds.add(BlobId.of(container, key));
            }
            if (!blobIds.isEmpty()) {
                storage.delete(blobIds);
            }
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to remove objects: " + e.getMessage(), e);
        }
    }

    @Override
    protected void copyObject(String fromContainer, String fromKey,
                              String toContainer, String toKey) {
        try {
            Storage.CopyRequest request = Storage.CopyRequest.newBuilder()
                    .setSource(BlobId.of(fromContainer, fromKey))
                    .setTarget(BlobId.of(toContainer, toKey))
                    .build();
            storage.copy(request).getResult();
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to copy object from " + fromContainer + "/" + fromKey
                            + " to " + toContainer + "/" + toKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String generateSignedGetUrl(String container, String objectKey, int ttlSeconds) {
        try {
            int effectiveTtl = Math.min(ttlSeconds, maxSignedUrlTTL);
            BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(container, objectKey)).build();
            URL url = storage.signUrl(blobInfo, effectiveTtl, TimeUnit.SECONDS,
                    Storage.SignUrlOption.httpMethod(HttpMethod.GET),
                    Storage.SignUrlOption.withV4Signature());
            return url.toString();
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to generate signed GET URL for: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String generateSignedPutUrl(String container, String objectKey,
                                          int ttlSeconds, String contentType,
                                          Map<String, String> additionalParams) {
        try {
            int effectiveTtl = Math.min(ttlSeconds, maxSignedUrlTTL);
            BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(container, objectKey)).build();

            // Check for chunked upload mode
            boolean isChunkedUpload = additionalParams != null
                    && "true".equalsIgnoreCase(additionalParams.get("chunked"));

            // If additionalParams contains service account credentials, use them
            Storage signingStorage = resolveSigningStorage(additionalParams);

            if (isChunkedUpload) {
                Map<String, String> extensionHeaders = new HashMap<>();
                extensionHeaders.put("Content-Type",
                        contentType != null ? contentType : "application/octet-stream");
                extensionHeaders.put("x-goog-resumable", "start");
                URL url = signingStorage.signUrl(blobInfo, effectiveTtl, TimeUnit.SECONDS,
                        Storage.SignUrlOption.httpMethod(HttpMethod.POST),
                        Storage.SignUrlOption.withV4Signature(),
                        Storage.SignUrlOption.withExtHeaders(extensionHeaders));
                return url.toString();
            } else {
                URL url = signingStorage.signUrl(blobInfo, effectiveTtl, TimeUnit.SECONDS,
                        Storage.SignUrlOption.httpMethod(HttpMethod.PUT),
                        Storage.SignUrlOption.withV4Signature());
                return url.toString();
            }
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to generate signed PUT URL for: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    /**
     * If additionalParams contains GCP service account credentials, build a dedicated
     * Storage client for signing. Otherwise fall back to the main storage client.
     */
    private Storage resolveSigningStorage(Map<String, String> additionalParams) {
        if (additionalParams == null || additionalParams.isEmpty()) {
            return storage;
        }
        String clientId = additionalParams.get("clientId");
        String clientEmail = additionalParams.get("clientEmail");
        String privateKeyPkcs8 = additionalParams.get("privateKeyPkcs8");
        String privateKeyId = additionalParams.get("privateKeyIds");
        String projectId = additionalParams.get("projectId");

        if (clientEmail != null && privateKeyPkcs8 != null && privateKeyId != null) {
            try {
                ServiceAccountCredentials credentials = ServiceAccountCredentials.fromPkcs8(
                        clientId, clientEmail, privateKeyPkcs8, privateKeyId,
                        new ArrayList<>());
                return StorageOptions.newBuilder()
                        .setProjectId(projectId)
                        .setCredentials(credentials)
                        .build()
                        .getService();
            } catch (IOException e) {
                throw new StorageServiceException(
                        "Failed to create signing credentials from additionalParams: " + e.getMessage(), e);
            }
        }
        return storage;
    }

    @Override
    protected String getHdfsPrefix(String container) {
        return "gs://" + container + "/";
    }

    @Override
    public String getUri(String container, String prefix, boolean isDirectory) {
        List<String> keys = listObjectKeys(container, prefix);
        if (keys.isEmpty()) {
            throw new StorageServiceException("The given prefix is incorrect: " + prefix);
        }
        return GCS_HOST + container + "/" + prefix;
    }

    @Override
    public void close() {
        try {
            storage.close();
        } catch (Exception e) {
            logger.warn("Error closing GCP Storage client", e);
        }
    }
}
