package org.sunbird.cloud.storage;

import org.sunbird.cloud.storage.model.Blob;
import org.sunbird.cloud.storage.model.DeleteTarget;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Cloud storage service interface providing operations for upload, download,
 * signed URL generation, deletion, copy, search, and metadata retrieval.
 *
 * <p>Implementations are CSP-specific (AWS, Azure, GCP, OCI) and discovered
 * via {@link StorageServiceFactory} using Java ServiceLoader.</p>
 */
public interface IStorageService extends AutoCloseable {

    // --- Upload operations ---

    /**
     * Upload a file or folder to cloud storage.
     *
     * @param container   the container/bucket name
     * @param file        the local file path
     * @param objectKey   the destination key/path in cloud storage
     * @param isDirectory whether to upload recursively as a directory
     * @param attempt     current attempt number (starting from 1)
     * @param retryCount  maximum number of retries before failing
     * @param ttl         time-to-live in seconds for the uploaded object (null for no expiry)
     * @return the URL of the uploaded file
     */
    String upload(String container, String file, String objectKey,
                  boolean isDirectory, int attempt, int retryCount, Integer ttl);

    default String upload(String container, String file, String objectKey) {
        return upload(container, file, objectKey, false, 1, 0, null);
    }

    /**
     * Upload a folder asynchronously.
     *
     * @param container  the container/bucket name
     * @param file       the local folder path
     * @param objectKey  the destination prefix in cloud storage
     * @param isPublic   whether uploaded files should have public read access
     * @param ttl        time-to-live in seconds (null for no expiry)
     * @param retryCount maximum number of retries (null for default)
     * @param attempt    current attempt number
     * @return future completing with a list of uploaded file URLs
     */
    CompletableFuture<List<String>> uploadFolder(String container, String file,
                                                  String objectKey, boolean isPublic,
                                                  Integer ttl, Integer retryCount, int attempt);

    default CompletableFuture<List<String>> uploadFolder(String container, String file,
                                                          String objectKey) {
        return uploadFolder(container, file, objectKey, false, null, null, 1);
    }

    /**
     * Put binary content directly as a blob in cloud storage.
     *
     * @param container   the container/bucket name
     * @param content     the byte array payload
     * @param objectKey   the destination key/path
     * @param isPublic    whether the object should have public read access
     * @param isDirectory whether the content represents a directory
     * @param ttl         time-to-live in seconds (null for no expiry)
     * @param retryCount  maximum number of retries (null for default)
     * @return the URL of the uploaded object
     */
    String put(String container, byte[] content, String objectKey,
               boolean isPublic, boolean isDirectory, Integer ttl, Integer retryCount);

    default String put(String container, byte[] content, String objectKey) {
        return put(container, content, objectKey, false, false, null, null);
    }

    // --- Signed URL operations ---

    /**
     * Get a pre-signed URL to access an object.
     *
     * @param container  the container/bucket name
     * @param objectKey  the object key
     * @param ttl        time-to-live in seconds for the URL (null for default)
     * @param permission "r" for read, "w" for write
     * @return the pre-signed URL
     */
    String getSignedURL(String container, String objectKey, Integer ttl, String permission);

    default String getSignedURL(String container, String objectKey) {
        return getSignedURL(container, objectKey, null, "r");
    }

    /**
     * Get a pre-signed URL with additional options (content type, extra params).
     *
     * @param container        the container/bucket name
     * @param objectKey        the object key
     * @param ttl              time-to-live in seconds (null for default)
     * @param permission       "r" for read, "w" for write
     * @param contentType      the content type of the object
     * @param additionalParams CSP-specific additional parameters (nullable)
     * @return the pre-signed URL
     */
    String getSignedURLV2(String container, String objectKey, Integer ttl,
                          String permission, String contentType,
                          Map<String, String> additionalParams);

    default String getSignedURLV2(String container, String objectKey) {
        return getSignedURLV2(container, objectKey, null, "r", "application/octet-stream", null);
    }

    // --- Download operations ---

    /**
     * Download a file or folder from cloud storage.
     *
     * @param container   the container/bucket name
     * @param objectKey   the object key to download
     * @param localPath   the local destination path
     * @param isDirectory whether to download recursively as a directory
     */
    void download(String container, String objectKey, String localPath, boolean isDirectory);

    default void download(String container, String objectKey, String localPath) {
        download(container, objectKey, localPath, false);
    }

    // --- Delete operations ---

    /**
     * Delete an object from cloud storage.
     *
     * @param container   the container/bucket name
     * @param objectKey   the object key to delete
     * @param isDirectory whether the object is a directory (recursive delete)
     */
    void deleteObject(String container, String objectKey, boolean isDirectory);

    default void deleteObject(String container, String objectKey) {
        deleteObject(container, objectKey, false);
    }

    /**
     * Delete multiple objects from cloud storage.
     *
     * @param container  the container/bucket name
     * @param objectKeys list of delete targets
     */
    void deleteObjects(String container, List<DeleteTarget> objectKeys);

    // --- Copy operations ---

    /**
     * Copy objects between containers or within the same container.
     *
     * @param fromContainer source container
     * @param fromKey       source object key/prefix
     * @param toContainer   destination container
     * @param toKey         destination object key/prefix
     * @param isDirectory   whether to copy recursively as a directory
     */
    void copyObjects(String fromContainer, String fromKey,
                     String toContainer, String toKey, boolean isDirectory);

    default void copyObjects(String fromContainer, String fromKey,
                             String toContainer, String toKey) {
        copyObjects(fromContainer, fromKey, toContainer, toKey, false);
    }

    // --- Archive operations ---

    /**
     * Extract an archive file on cloud storage to a destination folder.
     *
     * @param container the container/bucket name
     * @param objectKey the archive object key
     * @param toKey     the destination folder key
     */
    void extractArchive(String container, String objectKey, String toKey);

    // --- Object retrieval ---

    /**
     * Get blob object details.
     *
     * @param container   the container/bucket name
     * @param objectKey   the object key
     * @param withPayload whether to include the payload bytes
     * @return the blob object with metadata
     */
    Blob getObject(String container, String objectKey, boolean withPayload);

    default Blob getObject(String container, String objectKey) {
        return getObject(container, objectKey, false);
    }

    /**
     * Get object data as lines of text.
     *
     * @param container the container/bucket name
     * @param objectKey the object key
     * @return the object content as a list of lines
     */
    List<String> getObjectData(String container, String objectKey);

    /**
     * List blob objects for a given prefix.
     *
     * @param container   the container/bucket name
     * @param prefix      the object prefix to list
     * @param withPayload whether to include payloads
     * @return list of blob objects
     */
    List<Blob> listObjects(String container, String prefix, boolean withPayload);

    default List<Blob> listObjects(String container, String prefix) {
        return listObjects(container, prefix, false);
    }

    /**
     * List object keys for a given prefix.
     *
     * @param container the container/bucket name
     * @param prefix    the object prefix
     * @return list of object keys
     */
    List<String> listObjectKeys(String container, String prefix);

    // --- Search operations ---

    /**
     * Search for objects by date-prefixed paths.
     *
     * @param container the container/bucket name
     * @param prefix    the base prefix
     * @param fromDate  start date string (nullable)
     * @param toDate    end date string (nullable)
     * @param delta     number of days delta from toDate (nullable)
     * @param pattern   date format pattern (e.g., "yyyy-MM-dd")
     * @return list of matching blob objects
     */
    List<Blob> searchObjects(String container, String prefix,
                             String fromDate, String toDate,
                             Integer delta, String pattern);

    default List<Blob> searchObjects(String container, String prefix) {
        return searchObjects(container, prefix, null, null, null, "yyyy-MM-dd");
    }

    /**
     * Search for object keys by date-prefixed paths.
     *
     * @param container the container/bucket name
     * @param prefix    the base prefix
     * @param fromDate  start date string (nullable)
     * @param toDate    end date string (nullable)
     * @param delta     number of days delta from toDate (nullable)
     * @param pattern   date format pattern (e.g., "yyyy-MM-dd")
     * @return list of matching object keys
     */
    List<String> searchObjectKeys(String container, String prefix,
                                  String fromDate, String toDate,
                                  Integer delta, String pattern);

    default List<String> searchObjectKeys(String container, String prefix) {
        return searchObjectKeys(container, prefix, null, null, null, "yyyy-MM-dd");
    }

    // --- Path operations ---

    /**
     * Get HDFS-compatible file paths for use with Spark etc.
     *
     * @param container the container/bucket name
     * @param objects   list of blob objects
     * @return HDFS-compatible paths
     */
    List<String> getPaths(String container, List<Blob> objects);

    /**
     * Get the URI for a given prefix.
     *
     * @param container   the container/bucket name
     * @param prefix      the object prefix
     * @param isDirectory whether the prefix represents a directory
     * @return the URI string
     */
    String getUri(String container, String prefix, boolean isDirectory);

    default String getUri(String container, String prefix) {
        return getUri(container, prefix, false);
    }

    // --- Lifecycle ---

    @Override
    void close();
}
