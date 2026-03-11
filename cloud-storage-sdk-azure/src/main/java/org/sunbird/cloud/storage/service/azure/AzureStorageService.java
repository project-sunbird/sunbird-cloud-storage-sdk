package org.sunbird.cloud.storage.service.azure;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cloud.storage.AbstractStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.exception.StorageServiceException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Azure Blob Storage implementation using Azure SDK for Java.
 */
public class AzureStorageService extends AbstractStorageService {

    private static final Logger logger = LoggerFactory.getLogger(AzureStorageService.class);

    private final StorageConfig config;
    private final BlobServiceClient blobServiceClient;
    private final boolean useSharedKey;

    public AzureStorageService(StorageConfig config) {
        this.config = config;

        String accountName = config.getStorageKey();
        String endpoint = "https://" + accountName + ".blob.core.windows.net";

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder().endpoint(endpoint);

        switch (config.getAuthType()) {
            case ACCESS_KEY:
                StorageSharedKeyCredential sharedKey =
                        new StorageSharedKeyCredential(accountName, config.getStorageSecret());
                builder.credential(sharedKey);
                this.useSharedKey = true;
                break;
            case OIDC:
                String clientId = System.getenv("AZURE_CLIENT_ID");
                String tenantId = System.getenv("AZURE_TENANT_ID");
                String tokenFile = System.getenv("AZURE_FEDERATED_TOKEN_FILE");
                if (clientId != null && tenantId != null && tokenFile != null) {
                    builder.credential(new WorkloadIdentityCredentialBuilder()
                            .clientId(clientId)
                            .tenantId(tenantId)
                            .tokenFilePath(tokenFile)
                            .build());
                } else {
                    builder.credential(new DefaultAzureCredentialBuilder().build());
                }
                this.useSharedKey = false;
                break;
            case IAM:
            case IAM_ROLE:
            case INSTANCE_PROFILE:
                builder.credential(new ManagedIdentityCredentialBuilder().build());
                this.useSharedKey = false;
                break;
            default:
                throw new StorageServiceException("Unsupported auth type: " + config.getAuthType());
        }

        this.blobServiceClient = builder.buildClient();
        logger.info("Initialized AzureStorageService for account={}, authType={}",
                accountName, config.getAuthType());
    }

    private BlobContainerClient getContainerClient(String container) {
        return blobServiceClient.getBlobContainerClient(container);
    }

    private BlobClient getBlobClient(String container, String objectKey) {
        return getContainerClient(container).getBlobClient(objectKey);
    }

    /**
     * The Azure SDK's getBlobUrl() encodes '/' in blob names as '%2F'.
     * This replaces only %2F with '/', leaving all other percent-encoded characters
     */
    private String decodeBlobUrl(String url) {
        try {
            java.net.URI uri = new java.net.URI(url);
            String path = uri.getRawPath().replace("%2F", "/").replace("%2f", "/");
            String result = uri.getScheme() + "://" + uri.getRawAuthority() + path;
            return uri.getRawQuery() != null ? result + "?" + uri.getRawQuery() : result;
        } catch (java.net.URISyntaxException e) {
            return url;
        }
    }

    @Override
    protected void ensureContainerExists(String container) {
        try {
            BlobContainerClient containerClient = getContainerClient(container);
            if (!containerClient.exists()) {
                containerClient.create();
                logger.info("Created container: {}", container);
            }
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to ensure container exists: " + container + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String putObject(String container, String objectKey, File file) {
        try {
            BlobClient blobClient = getBlobClient(container, objectKey);
            String contentType = tika.detect(file);
            blobClient.uploadFromFileWithResponse(
                    new com.azure.storage.blob.options.BlobUploadFromFileOptions(file.getAbsolutePath())
                            .setHeaders(new BlobHttpHeaders().setContentType(contentType)),
                    null, null);
            return decodeBlobUrl(blobClient.getBlobUrl());
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to put object from file: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String putObject(String container, String objectKey, byte[] content) {
        try {
            BlobClient blobClient = getBlobClient(container, objectKey);
            String contentType = tika.detect(new ByteArrayInputStream(content), objectKey);
            blobClient.uploadWithResponse(
                    new com.azure.storage.blob.options.BlobParallelUploadOptions(
                            new ByteArrayInputStream(content), content.length)
                            .setHeaders(new BlobHttpHeaders().setContentType(contentType)),
                    null, null);
            return decodeBlobUrl(blobClient.getBlobUrl());
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to put object from bytes: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected InputStream getObjectStream(String container, String objectKey) {
        try {
            BlobClient blobClient = getBlobClient(container, objectKey);
            return blobClient.openInputStream();
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object stream: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String getObjectUri(String container, String objectKey) {
        return decodeBlobUrl(getBlobClient(container, objectKey).getBlobUrl());
    }

    @Override
    protected BlobDetail getObjectDetail(String container, String objectKey) {
        try {
            BlobClient blobClient = getBlobClient(container, objectKey);
            BlobProperties props = blobClient.getProperties();

            Map<String, Object> metadata = new HashMap<>();
            if (props.getMetadata() != null) {
                metadata.putAll(props.getMetadata());
            }
            String blobUrl = decodeBlobUrl(blobClient.getBlobUrl());
            metadata.put("uri", blobUrl);
            metadata.put("Content-Type", props.getContentType());
            metadata.put("ETag", props.getETag());

            Date lastModified = props.getLastModified() != null
                    ? Date.from(props.getLastModified().toInstant())
                    : null;

            return new BlobDetail(objectKey, props.getBlobSize(), lastModified,
                    metadata, blobUrl);
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to get object detail: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected List<String> listKeys(String container, String prefix) {
        try {
            BlobContainerClient containerClient = getContainerClient(container);
            ListBlobsOptions options = new ListBlobsOptions()
                    .setPrefix(prefix)
                    .setDetails(new BlobListDetails().setRetrieveMetadata(false));

            List<String> keys = new ArrayList<>();
            for (BlobItem item : containerClient.listBlobs(options, null)) {
                keys.add(item.getName());
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
            for (String key : objectKeys) {
                BlobClient blobClient = getBlobClient(container, key);
                blobClient.deleteIfExists();
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
            BlobClient sourceBlobClient = getBlobClient(fromContainer, fromKey);
            BlobClient destBlobClient = getBlobClient(toContainer, toKey);
            destBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl());
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to copy object from " + fromContainer + "/" + fromKey
                            + " to " + toContainer + "/" + toKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String generateSignedGetUrl(String container, String objectKey, int ttlSeconds) {
        try {
            BlobClient blobClient = getBlobClient(container, objectKey);
            BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusSeconds(ttlSeconds), permission);

            String baseUrl = decodeBlobUrl(blobClient.getBlobUrl());
            if (useSharedKey) {
                return baseUrl + "?" + blobClient.generateSas(values);
            } else {
                var delegationKey = blobServiceClient.getUserDelegationKey(
                        OffsetDateTime.now().minusMinutes(5),
                        OffsetDateTime.now().plusSeconds(ttlSeconds));
                return baseUrl + "?" + blobClient.generateUserDelegationSas(values, delegationKey);
            }
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
            BlobClient blobClient = getBlobClient(container, objectKey);
            BlobSasPermission permission = new BlobSasPermission()
                    .setWritePermission(true)
                    .setCreatePermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusSeconds(ttlSeconds), permission);
            if (contentType != null) {
                values.setContentType(contentType);
            }

            String baseUrl = decodeBlobUrl(blobClient.getBlobUrl());
            if (useSharedKey) {
                return baseUrl + "?" + blobClient.generateSas(values);
            } else {
                var delegationKey = blobServiceClient.getUserDelegationKey(
                        OffsetDateTime.now().minusMinutes(5),
                        OffsetDateTime.now().plusSeconds(ttlSeconds));
                return baseUrl + "?" + blobClient.generateUserDelegationSas(values, delegationKey);
            }
        } catch (Exception e) {
            throw new StorageServiceException(
                    "Failed to generate signed PUT URL for: " + objectKey + " - " + e.getMessage(), e);
        }
    }

    @Override
    protected String getHdfsPrefix(String container) {
        return "wasb://" + container + "@" + config.getStorageKey() + ".blob.core.windows.net/";
    }

    @Override
    public void close() {
        // BlobServiceClient does not implement Closeable; no resource cleanup needed
        logger.info("AzureStorageService closed");
    }
}
