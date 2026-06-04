package org.sunbird.cloud.storage.service.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cloud.storage.AbstractStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.exception.StorageServiceException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AWS S3 storage service implementation using AWS SDK v2.
 *
 * <p>Supports standard AWS S3, Ceph S3-compatible, and OCI S3-compatible storage
 * endpoints. Authentication can be configured via access keys, OIDC, IAM roles,
 * or instance profiles.</p>
 */
public class AwsStorageService extends AbstractStorageService {

    private static final Logger logger = LoggerFactory.getLogger(AwsStorageService.class);

    private final StorageConfig config;
    private final S3Client s3Client;
    private final S3Presigner s3Presigner;

    public AwsStorageService(StorageConfig config) {
        this.config = config;

        Region region = (config.getRegion() != null && !config.getRegion().isEmpty())
                ? Region.of(config.getRegion())
                : Region.US_EAST_1;

        boolean useCustomEndpoint = isCustomEndpoint(config);
        String endPoint = config.getEndPoint();

        // Build S3Client
        S3ClientBuilder clientBuilder = S3Client.builder().region(region);
        S3Presigner.Builder presignerBuilder = S3Presigner.builder().region(region);

        // Configure credentials based on auth type
        switch (config.getAuthType()) {
            case ACCESS_KEY:
                StaticCredentialsProvider staticCreds = StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.getStorageKey(), config.getStorageSecret()));
                clientBuilder.credentialsProvider(staticCreds);
                presignerBuilder.credentialsProvider(staticCreds);
                break;
            case OIDC:
            case IAM:
            case IAM_ROLE:
            case INSTANCE_PROFILE:
                DefaultCredentialsProvider defaultCreds = DefaultCredentialsProvider.create();
                clientBuilder.credentialsProvider(defaultCreds);
                presignerBuilder.credentialsProvider(defaultCreds);
                break;
            default:
                throw new StorageServiceException("Unsupported auth type: " + config.getAuthType());
        }

        // Configure custom endpoint and path-style access for non-AWS S3 implementations
        if (useCustomEndpoint && endPoint != null && !endPoint.isEmpty()) {
            URI endpointUri = URI.create(endPoint);
            clientBuilder.endpointOverride(endpointUri);
            presignerBuilder.endpointOverride(endpointUri);

            S3Configuration s3Config = S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .build();
            clientBuilder.serviceConfiguration(s3Config);
            presignerBuilder.serviceConfiguration(s3Config);
        }

        this.s3Client = clientBuilder.build();
        this.s3Presigner = presignerBuilder.build();

        logger.info("Initialized AwsStorageService for type={}, region={}, customEndpoint={}",
                config.getType(), region, useCustomEndpoint);
    }

    /**
     * Determines whether a custom endpoint should be configured.
     * Custom endpoints are used for CEPHS3, OCI, or when an explicit endpoint is set.
     */
    private boolean isCustomEndpoint(StorageConfig config) {
        if (config.getType() == StorageConfig.StorageType.CEPHS3
                || config.getType() == StorageConfig.StorageType.OCI) {
            return true;
        }
        return config.getEndPoint() != null && !config.getEndPoint().isEmpty();
    }

    @Override
    protected void ensureContainerExists(String container) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(container).build());
        } catch (NoSuchBucketException e) {
            try {
                s3Client.createBucket(CreateBucketRequest.builder().bucket(container).build());
                logger.info("Created bucket: {}", container);
            } catch (BucketAlreadyOwnedByYouException ignored) {
                // Bucket was created concurrently; safe to proceed
                logger.debug("Bucket already owned by us: {}", container);
            } catch (Exception ex) {
                throw new StorageServiceException(
                        "Failed to create bucket: " + container + " - " + ex.getMessage(), ex);
            }
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to check bucket existence: " + container + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String putObject(String container, String objectKey, File file) {
        try {
            String contentType = tika.detect(file);
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(container)
                    .key(objectKey)
                    .contentType(contentType)
                    .build();
            s3Client.putObject(request, RequestBody.fromFile(file));
            return getObjectUri(container, objectKey);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to put object from file: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String putObject(String container, String objectKey, byte[] content) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(container)
                    .key(objectKey)
                    .contentType(tika.detect(new java.io.ByteArrayInputStream(content), objectKey))
                    .build();
            s3Client.putObject(request, RequestBody.fromBytes(content));
            return getObjectUri(container, objectKey);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to put object from bytes: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected InputStream getObjectStream(String container, String objectKey) {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(container)
                    .key(objectKey)
                    .build();
            return s3Client.getObject(request);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object stream: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String getObjectUri(String container, String objectKey) {
        String endPoint = config.getEndPoint();
        if (endPoint != null && !endPoint.isEmpty()) {
            // Custom endpoint: <endPoint>/<container>/<objectKey>
            String base = endPoint.endsWith("/") ? endPoint.substring(0, endPoint.length() - 1) : endPoint;
            return base + "/" + container + "/" + objectKey;
        }
        // Standard AWS S3: https://<container>.s3.amazonaws.com/<objectKey>
        return "https://" + container + ".s3.amazonaws.com/" + objectKey;
    }

    @Override
    protected BlobDetail getObjectDetail(String container, String objectKey) {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(container)
                    .key(objectKey)
                    .build();
            HeadObjectResponse response = s3Client.headObject(request);

            long contentLength = response.contentLength() != null ? response.contentLength() : 0L;
            Date lastModified = response.lastModified() != null
                    ? Date.from(response.lastModified())
                    : null;

            Map<String, Object> metadata = new HashMap<>();
            if (response.metadata() != null) {
                metadata.putAll(response.metadata());
            }
            if (response.contentType() != null) {
                metadata.put("Content-Type", response.contentType());
            }
            if (response.eTag() != null) {
                metadata.put("ETag", response.eTag());
            }

            String uri = getObjectUri(container, objectKey);
            return new BlobDetail(objectKey, contentLength, lastModified, metadata, uri);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object detail: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected List<String> listKeys(String container, String prefix) {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(container)
                    .prefix(prefix)
                    .build();
            ListObjectsV2Iterable pages = s3Client.listObjectsV2Paginator(request);

            List<String> keys = new ArrayList<>();
            for (var page : pages) {
                for (S3Object s3Object : page.contents()) {
                    keys.add(s3Object.key());
                }
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
            if (objectKeys == null || objectKeys.isEmpty()) {
                return;
            }

            // S3 DeleteObjects supports up to 1000 keys per request
            int batchSize = 1000;
            for (int i = 0; i < objectKeys.size(); i += batchSize) {
                List<String> batch = objectKeys.subList(i, Math.min(i + batchSize, objectKeys.size()));
                List<ObjectIdentifier> identifiers = batch.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .collect(Collectors.toList());

                DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                        .bucket(container)
                        .delete(Delete.builder().objects(identifiers).quiet(true).build())
                        .build();
                s3Client.deleteObjects(request);
            }
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to remove objects from container: " + container + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected void copyObject(String fromContainer, String fromKey,
                               String toContainer, String toKey) {
        try {
            CopyObjectRequest request = CopyObjectRequest.builder()
                    .sourceBucket(fromContainer)
                    .sourceKey(fromKey)
                    .destinationBucket(toContainer)
                    .destinationKey(toKey)
                    .build();
            s3Client.copyObject(request);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to copy object from " + fromContainer + "/" + fromKey
                            + " to " + toContainer + "/" + toKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String generateSignedGetUrl(String container, String objectKey, int ttlSeconds) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(container)
                    .key(objectKey)
                    .build();
            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(ttlSeconds))
                    .getObjectRequest(getObjectRequest)
                    .build();
            return s3Presigner.presignGetObject(presignRequest).url().toString();
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
            PutObjectRequest.Builder putRequestBuilder = PutObjectRequest.builder()
                    .bucket(container)
                    .key(objectKey);
            if (contentType != null && !contentType.isEmpty()) {
                putRequestBuilder.contentType(contentType);
            }
            if (additionalParams != null && !additionalParams.isEmpty()) {
                putRequestBuilder.metadata(additionalParams);
            }

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(ttlSeconds))
                    .putObjectRequest(putRequestBuilder.build())
                    .build();
            return s3Presigner.presignPutObject(presignRequest).url().toString();
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to generate signed PUT URL for: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String getHdfsPrefix(String container) {
        return "s3n://" + container + "/";
    }

    @Override
    public void close() {
        try {
            if (s3Client != null) {
                s3Client.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing S3Client", e);
        }
        try {
            if (s3Presigner != null) {
                s3Presigner.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing S3Presigner", e);
        }
    }
}
