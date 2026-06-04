package org.sunbird.cloud.storage.service.oci;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.model.CreatePreauthenticatedRequestDetails;
import com.oracle.bmc.objectstorage.model.ListObjects;
import com.oracle.bmc.objectstorage.model.ObjectSummary;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.responses.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cloud.storage.AbstractStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.exception.StorageServiceException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle Cloud Infrastructure Object Storage implementation using the OCI Java SDK.
 */
public class OciStorageService extends AbstractStorageService {

    private static final Logger logger = LoggerFactory.getLogger(OciStorageService.class);

    private final StorageConfig config;
    private final ObjectStorageClient objectStorageClient;
    private final String namespace;

    public OciStorageService(StorageConfig config) {
        this.config = config;

        BasicAuthenticationDetailsProvider authProvider = resolveAuth(config);

        ObjectStorageClient.Builder clientBuilder = ObjectStorageClient.builder();

        if (config.getEndPoint() != null && !config.getEndPoint().isEmpty()) {
            clientBuilder.endpoint(config.getEndPoint());
        } else if (config.getRegion() != null && !config.getRegion().isEmpty()) {
            clientBuilder.region(Region.fromRegionId(config.getRegion()));
        }

        ObjectStorageClient tempClient = clientBuilder.build(authProvider);
        try {
            GetNamespaceResponse namespaceResponse = tempClient.getNamespace(
                    GetNamespaceRequest.builder().build());
            this.namespace = namespaceResponse.getValue();
            this.objectStorageClient = tempClient;
        } catch (Exception e) {
            tempClient.close();
            throw new StorageServiceException(
                    "Failed to initialize OCI storage - could not get namespace: " + e.getMessage(), e);
        }

        logger.info("Initialized OciStorageService, namespace={}, authType={}",
                namespace, config.getAuthType());
    }

    private BasicAuthenticationDetailsProvider resolveAuth(StorageConfig config) {
        switch (config.getAuthType()) {
            case ACCESS_KEY:
                // For API Key auth, storageKey is the user OCID, storageSecret is the private key PEM
                // This requires additional config values typically set via environment or system properties
                String tenancyId = System.getenv().getOrDefault("OCI_TENANCY_ID",
                        System.getProperty("oci.tenancy.id", ""));
                String userId = config.getStorageKey();
                String fingerprint = System.getenv().getOrDefault("OCI_FINGERPRINT",
                        System.getProperty("oci.fingerprint", ""));
                String privateKey = config.getStorageSecret();
                String region = config.getRegion() != null ? config.getRegion() : "us-ashburn-1";

                return SimpleAuthenticationDetailsProvider.builder()
                        .tenantId(tenancyId)
                        .userId(userId)
                        .fingerprint(fingerprint)
                        .privateKeySupplier(() -> new ByteArrayInputStream(privateKey.getBytes()))
                        .region(Region.fromRegionId(region))
                        .build();

            case OIDC:
            case IAM:
            case IAM_ROLE:
            case INSTANCE_PROFILE:
                return InstancePrincipalsAuthenticationDetailsProvider.builder().build();

            default:
                throw new StorageServiceException("Unsupported auth type: " + config.getAuthType());
        }
    }

    @Override
    protected void ensureContainerExists(String container) {
        try {
            objectStorageClient.headBucket(HeadBucketRequest.builder()
                    .namespaceName(namespace)
                    .bucketName(container)
                    .build());
        } catch (com.oracle.bmc.model.BmcException e) {
            if (e.getStatusCode() == 404) {
                try {
                    objectStorageClient.createBucket(CreateBucketRequest.builder()
                            .namespaceName(namespace)
                            .createBucketDetails(CreateBucketDetails.builder()
                                    .name(container)
                                    .compartmentId(System.getenv().getOrDefault(
                                            "OCI_COMPARTMENT_ID",
                                            System.getProperty("oci.compartment.id", "")))
                                    .build())
                            .build());
                    logger.info("Created bucket: {}", container);
                } catch (Exception ex) {
                    logger.debug("Bucket creation for {}: {}", container, ex.getMessage());
                }
            }
        }
    }

    @Override
    protected String putObject(String container, String objectKey, File file) {
        try {
            String contentType = tika.detect(file);
            byte[] data = Files.readAllBytes(file.toPath());
            String md5 = computeMd5Base64(data);

            PutObjectRequest request = PutObjectRequest.builder()
                    .namespaceName(namespace)
                    .bucketName(container)
                    .objectName(objectKey)
                    .contentType(contentType)
                    .contentLength((long) data.length)
                    .contentMD5(md5)
                    .putObjectBody(new ByteArrayInputStream(data))
                    .build();
            objectStorageClient.putObject(request);
            return buildObjectUri(container, objectKey);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to put object from file: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String putObject(String container, String objectKey, byte[] content) {
        try {
            String md5 = computeMd5Base64(content);
            PutObjectRequest request = PutObjectRequest.builder()
                    .namespaceName(namespace)
                    .bucketName(container)
                    .objectName(objectKey)
                    .contentLength((long) content.length)
                    .contentMD5(md5)
                    .putObjectBody(new ByteArrayInputStream(content))
                    .build();
            objectStorageClient.putObject(request);
            return buildObjectUri(container, objectKey);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to put object from bytes: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected InputStream getObjectStream(String container, String objectKey) {
        try {
            GetObjectResponse response = objectStorageClient.getObject(
                    GetObjectRequest.builder()
                            .namespaceName(namespace)
                            .bucketName(container)
                            .objectName(objectKey)
                            .build());
            return response.getInputStream();
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object stream: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String getObjectUri(String container, String objectKey) {
        return buildObjectUri(container, objectKey);
    }

    @Override
    protected BlobDetail getObjectDetail(String container, String objectKey) {
        try {
            HeadObjectResponse response = objectStorageClient.headObject(
                    HeadObjectRequest.builder()
                            .namespaceName(namespace)
                            .bucketName(container)
                            .objectName(objectKey)
                            .build());

            Map<String, Object> metadata = new HashMap<>();
            if (response.getOpcMeta() != null) {
                metadata.putAll(response.getOpcMeta());
            }
            metadata.put("uri", buildObjectUri(container, objectKey));
            metadata.put("Content-Type", response.getContentType());
            metadata.put("ETag", response.getETag());

            long contentLength = response.getContentLength() != null ? response.getContentLength() : 0L;
            Date lastModified = response.getLastModified() != null
                    ? Date.from(response.getLastModified().toInstant())
                    : null;

            return new BlobDetail(objectKey, contentLength, lastModified, metadata,
                    buildObjectUri(container, objectKey));
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object detail: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected List<String> listKeys(String container, String prefix) {
        try {
            List<String> keys = new ArrayList<>();
            String nextStart = null;

            do {
                ListObjectsRequest.Builder requestBuilder = ListObjectsRequest.builder()
                        .namespaceName(namespace)
                        .bucketName(container)
                        .prefix(prefix);
                if (nextStart != null) {
                    requestBuilder.start(nextStart);
                }

                ListObjectsResponse response = objectStorageClient.listObjects(requestBuilder.build());
                ListObjects listObjects = response.getListObjects();
                for (ObjectSummary summary : listObjects.getObjects()) {
                    keys.add(summary.getName());
                }
                nextStart = listObjects.getNextStartWith();
            } while (nextStart != null);

            return keys;
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to list keys with prefix: " + prefix + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected void removeObjects(String container, List<String> objectKeys) {
        try {
            for (String key : objectKeys) {
                objectStorageClient.deleteObject(DeleteObjectRequest.builder()
                        .namespaceName(namespace)
                        .bucketName(container)
                        .objectName(key)
                        .build());
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
            objectStorageClient.copyObject(CopyObjectRequest.builder()
                    .namespaceName(namespace)
                    .bucketName(fromContainer)
                    .copyObjectDetails(com.oracle.bmc.objectstorage.model.CopyObjectDetails.builder()
                            .sourceObjectName(fromKey)
                            .destinationNamespace(namespace)
                            .destinationBucket(toContainer)
                            .destinationObjectName(toKey)
                            .destinationRegion(config.getRegion() != null ? config.getRegion() : "us-ashburn-1")
                            .build())
                    .build());
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to copy object from " + fromContainer + "/" + fromKey
                            + " to " + toContainer + "/" + toKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String generateSignedGetUrl(String container, String objectKey, int ttlSeconds) {
        return generatePreauthenticatedRequest(container, objectKey, ttlSeconds,
                CreatePreauthenticatedRequestDetails.AccessType.ObjectRead);
    }

    @Override
    protected String generateSignedPutUrl(String container, String objectKey,
                                          int ttlSeconds, String contentType,
                                          Map<String, String> additionalParams) {
        return generatePreauthenticatedRequest(container, objectKey, ttlSeconds,
                CreatePreauthenticatedRequestDetails.AccessType.ObjectWrite);
    }

    private String generatePreauthenticatedRequest(String container, String objectKey,
                                                    int ttlSeconds,
                                                    CreatePreauthenticatedRequestDetails.AccessType accessType) {
        try {
            Date expiry = Date.from(java.time.Instant.now().plusSeconds(ttlSeconds));

            CreatePreauthenticatedRequestDetails details = CreatePreauthenticatedRequestDetails.builder()
                    .name("par-" + objectKey + "-" + System.currentTimeMillis())
                    .objectName(objectKey)
                    .accessType(accessType)
                    .timeExpires(expiry)
                    .build();

            CreatePreauthenticatedRequestResponse response =
                    objectStorageClient.createPreauthenticatedRequest(
                            CreatePreauthenticatedRequestRequest.builder()
                                    .namespaceName(namespace)
                                    .bucketName(container)
                                    .createPreauthenticatedRequestDetails(details)
                                    .build());

            String accessUri = response.getPreauthenticatedRequest().getAccessUri();
            String endpoint = config.getEndPoint() != null ? config.getEndPoint()
                    : "https://objectstorage." + (config.getRegion() != null ? config.getRegion() : "us-ashburn-1")
                    + ".oraclecloud.com";
            return endpoint + accessUri;
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to generate pre-authenticated request for: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String getHdfsPrefix(String container) {
        return "oci://" + container + "@" + namespace + "/";
    }

    @Override
    public void close() {
        try {
            objectStorageClient.close();
        } catch (Exception e) {
            logger.warn("Error closing OCI ObjectStorageClient", e);
        }
    }

    private String buildObjectUri(String container, String objectKey) {
        String endpoint = config.getEndPoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
            return base + "/n/" + namespace + "/b/" + container + "/o/" + objectKey;
        }
        String region = config.getRegion() != null ? config.getRegion() : "us-ashburn-1";
        return "https://objectstorage." + region + ".oraclecloud.com/n/" +
                namespace + "/b/" + container + "/o/" + objectKey;
    }

    private String computeMd5Base64(byte[] content) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(content);
            return Base64.getEncoder().encodeToString(md.digest());
        } catch (Exception e) {
            throw new StorageServiceException("Failed to compute MD5: " + e.getMessage(), e);
        }
    }
}
