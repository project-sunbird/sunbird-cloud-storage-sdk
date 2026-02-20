package org.sunbird.cloud.storage;

import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cloud.storage.exception.StorageServiceException;
import org.sunbird.cloud.storage.model.Blob;
import org.sunbird.cloud.storage.model.DeleteTarget;
import org.sunbird.cloud.storage.util.DateRangeUtil;
import org.sunbird.cloud.storage.util.FileUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Abstract base implementation of {@link IStorageService} providing shared orchestration logic.
 *
 * <p>CSP-specific subclasses implement the abstract primitive methods to interact with
 * the actual cloud storage APIs.</p>
 */
public abstract class AbstractStorageService implements IStorageService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractStorageService.class);

    protected final Tika tika = new Tika();
    protected int maxRetries = 2;
    protected int maxSignedUrlTTL = 604800;

    // --- Abstract primitives that each CSP module must implement ---

    protected abstract void ensureContainerExists(String container);

    protected abstract String putObject(String container, String objectKey, File file);

    protected abstract String putObject(String container, String objectKey, byte[] content);

    protected abstract InputStream getObjectStream(String container, String objectKey);

    protected abstract String getObjectUri(String container, String objectKey);

    protected abstract BlobDetail getObjectDetail(String container, String objectKey);

    protected abstract List<String> listKeys(String container, String prefix);

    protected abstract void removeObjects(String container, List<String> objectKeys);

    protected abstract void copyObject(String fromContainer, String fromKey,
                                       String toContainer, String toKey);

    protected abstract String generateSignedGetUrl(String container, String objectKey,
                                                   int ttlSeconds);

    protected abstract String generateSignedPutUrl(String container, String objectKey,
                                                   int ttlSeconds, String contentType,
                                                   Map<String, String> additionalParams);

    /**
     * Get the HDFS path prefix for this CSP (e.g., "s3n://", "gs://", "wasb://...").
     *
     * @param container the container/bucket name
     * @return the full HDFS prefix including the container
     */
    protected abstract String getHdfsPrefix(String container);

    /**
     * Metadata holder returned by {@link #getObjectDetail(String, String)}.
     */
    protected static class BlobDetail {
        public final String key;
        public final long contentLength;
        public final Date lastModified;
        public final Map<String, Object> metadata;
        public final String uri;

        public BlobDetail(String key, long contentLength, Date lastModified,
                          Map<String, Object> metadata, String uri) {
            this.key = key;
            this.contentLength = contentLength;
            this.lastModified = lastModified;
            this.metadata = metadata;
            this.uri = uri;
        }
    }

    // --- Orchestration implementations ---

    @Override
    public String upload(String container, String file, String objectKey,
                         boolean isDirectory, int attempt, int retryCount, Integer ttl) {
        int maxAttempts = retryCount > 0 ? retryCount : maxRetries;
        try {
            if (isDirectory) {
                File dir = new File(file);
                List<File> files = FileUtil.listFilesRecursive(dir);
                List<String> urls = new ArrayList<>();
                for (File f : files) {
                    String relativePath = f.getAbsolutePath()
                            .substring((dir.getAbsolutePath() + File.separator).length());
                    String key = objectKey + "/" + relativePath;
                    urls.add(upload(container, f.getAbsolutePath(), key, false, attempt, maxAttempts, ttl));
                }
                return String.join(",", urls);
            } else {
                if (attempt >= maxAttempts) {
                    throw new StorageServiceException(
                            "Failed to upload. file: " + file + ", key: " + objectKey +
                                    ", attempt: " + attempt + ", maxAttempts: " + maxAttempts +
                                    ". Exceeded maximum number of retries");
                }
                ensureContainerExists(container);
                String uri = putObject(container, objectKey, new File(file));
                if (ttl != null) {
                    return getSignedURL(container, objectKey, ttl, "r");
                }
                return uri;
            }
        } catch (StorageServiceException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Upload failed for key: {}, attempt: {}", objectKey, attempt, e);
            try {
                Thread.sleep((long) attempt * 2000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            int nextAttempt = attempt + 1;
            if (nextAttempt <= maxAttempts) {
                return upload(container, file, objectKey, isDirectory, nextAttempt, maxAttempts, ttl);
            }
            throw new StorageServiceException("Upload failed after " + attempt + " attempts: " + e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<List<String>> uploadFolder(String container, String file,
                                                         String objectKey, boolean isPublic,
                                                         Integer ttl, Integer retryCount,
                                                         int attempt) {
        return CompletableFuture.supplyAsync(() -> {
            File dir = new File(file);
            List<File> files = FileUtil.listFilesRecursive(dir);
            List<CompletableFuture<String>> futures = new ArrayList<>();
            for (File f : files) {
                String relativePath = f.getAbsolutePath()
                        .substring((dir.getAbsolutePath() + File.separator).length());
                String key = objectKey + "/" + relativePath;
                futures.add(CompletableFuture.supplyAsync(() ->
                        upload(container, f.getAbsolutePath(), key, false, attempt,
                                retryCount != null ? retryCount : 0, ttl)));
            }
            return futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());
        });
    }

    @Override
    public String put(String container, byte[] content, String objectKey,
                      boolean isPublic, boolean isDirectory, Integer ttl, Integer retryCount) {
        int maxAttempts = retryCount != null ? retryCount : maxRetries;
        int attempt = 0;
        while (true) {
            try {
                if (attempt >= maxAttempts) {
                    throw new StorageServiceException(
                            "Failed to put. key: " + objectKey +
                                    ", attempt: " + attempt + ", maxAttempts: " + maxAttempts +
                                    ". Exceeded maximum number of retries");
                }
                ensureContainerExists(container);
                String uri = putObject(container, objectKey, content);
                if (isPublic) {
                    return getSignedURL(container, objectKey,
                            ttl != null ? ttl : maxSignedUrlTTL, "r");
                }
                return uri;
            } catch (StorageServiceException e) {
                throw e;
            } catch (Exception e) {
                logger.error("Put failed for key: {}, attempt: {}", objectKey, attempt, e);
                try {
                    Thread.sleep((long) attempt * 2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                attempt++;
                if (attempt > maxAttempts) {
                    throw new StorageServiceException("Put failed: " + e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public String getSignedURL(String container, String objectKey, Integer ttl, String permission) {
        int effectiveTtl = ttl != null ? ttl : maxSignedUrlTTL;
        if ("w".equalsIgnoreCase(permission)) {
            return generateSignedPutUrl(container, objectKey, effectiveTtl, null, null);
        }
        return generateSignedGetUrl(container, objectKey, effectiveTtl);
    }

    @Override
    public String getSignedURLV2(String container, String objectKey, Integer ttl,
                                 String permission, String contentType,
                                 Map<String, String> additionalParams) {
        int effectiveTtl = ttl != null ? ttl : maxSignedUrlTTL;
        if ("w".equalsIgnoreCase(permission)) {
            return generateSignedPutUrl(container, objectKey, effectiveTtl, contentType, additionalParams);
        }
        return generateSignedGetUrl(container, objectKey, effectiveTtl);
    }

    @Override
    public void download(String container, String objectKey, String localPath, boolean isDirectory) {
        try {
            if (isDirectory) {
                List<String> objects = listObjectKeys(container, objectKey);
                for (String obj : objects) {
                    InputStream stream = getObjectStream(container, obj);
                    String relativePath = obj.startsWith(objectKey) ? obj.substring(objectKey.length()) : obj;
                    File targetFile = new File(localPath, relativePath);
                    File parentDir = targetFile.getParentFile();
                    if (parentDir != null && !parentDir.exists()) {
                        parentDir.mkdirs();
                    }
                    String fileName = targetFile.getName();
                    String dirPath = parentDir != null ? parentDir.getAbsolutePath() + "/" : localPath;
                    FileUtil.copyStream(stream, dirPath, fileName);
                }
            } else {
                InputStream stream = getObjectStream(container, objectKey);
                String fileName = objectKey.contains("/")
                        ? objectKey.substring(objectKey.lastIndexOf('/') + 1)
                        : objectKey;
                FileUtil.copyStream(stream, localPath, fileName);
            }
        } catch (StorageServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageServiceException("Download failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObject(String container, String objectKey, boolean isDirectory) {
        deleteObjects(container, List.of(new DeleteTarget(objectKey, isDirectory)));
    }

    @Override
    public void deleteObjects(String container, List<DeleteTarget> objectKeys) {
        try {
            for (DeleteTarget target : objectKeys) {
                if (target.isDirectory()) {
                    List<String> keys = listKeys(container, target.getObjectKey());
                    if (!keys.isEmpty()) {
                        removeObjects(container, keys);
                    }
                } else {
                    removeObjects(container, List.of(target.getObjectKey()));
                }
            }
        } catch (StorageServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageServiceException("Delete failed: " + e.getMessage(), e);
        }
    }

    @Override
    public Blob getObject(String container, String objectKey, boolean withPayload) {
        try {
            BlobDetail detail = getObjectDetail(container, objectKey);
            byte[] payload = null;
            if (withPayload) {
                try (InputStream is = getObjectStream(container, objectKey)) {
                    payload = is.readAllBytes();
                }
            }
            return new Blob(objectKey, detail.contentLength, detail.lastModified,
                    detail.metadata, payload);
        } catch (StorageServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageServiceException("Failed to get object: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getObjectData(String container, String objectKey) {
        try (InputStream is = getObjectStream(container, objectKey);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.toList());
        } catch (IOException e) {
            throw new StorageServiceException("Failed to read object data: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Blob> listObjects(String container, String prefix, boolean withPayload) {
        try {
            List<String> keys = listObjectKeys(container, prefix);
            List<Blob> blobs = new ArrayList<>();
            for (String key : keys) {
                blobs.add(getObject(container, key, withPayload));
            }
            return blobs;
        } catch (StorageServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageServiceException("Failed to list objects: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listObjectKeys(String container, String prefix) {
        return listKeys(container, prefix);
    }

    @Override
    public List<Blob> searchObjects(String container, String prefix,
                                    String fromDate, String toDate,
                                    Integer delta, String pattern) {
        String from = (delta != null) ? DateRangeUtil.getStartDate(toDate, delta) : fromDate;
        if (from != null && !from.isEmpty()) {
            String[] dates = DateRangeUtil.getDatesBetween(from, toDate, pattern);
            List<Blob> results = new ArrayList<>();
            for (String date : dates) {
                results.addAll(listObjects(container, prefix + date));
            }
            return results;
        }
        return listObjects(container, prefix);
    }

    @Override
    public List<String> searchObjectKeys(String container, String prefix,
                                         String fromDate, String toDate,
                                         Integer delta, String pattern) {
        String from = (delta != null) ? DateRangeUtil.getStartDate(toDate, delta) : fromDate;
        if (from != null && !from.isEmpty()) {
            String[] dates = DateRangeUtil.getDatesBetween(from, toDate, pattern);
            List<String> results = new ArrayList<>();
            for (String date : dates) {
                results.addAll(listObjectKeys(container, prefix + date));
            }
            return results;
        }
        return listObjectKeys(container, prefix);
    }

    @Override
    public void copyObjects(String fromContainer, String fromKey,
                            String toContainer, String toKey, boolean isDirectory) {
        if (isDirectory) {
            String updatedFromKey = fromKey.endsWith("/") ? fromKey : fromKey + "/";
            String updatedToKey = toKey.endsWith("/") ? toKey : toKey + "/";
            List<String> keys = listObjectKeys(fromContainer, updatedFromKey);
            for (String key : keys) {
                String objName = key.replace(updatedFromKey, "");
                copyObject(fromContainer, key, toContainer, updatedToKey + objName);
            }
        } else {
            copyObject(fromContainer, fromKey, toContainer, toKey);
        }
    }

    @Override
    public void extractArchive(String container, String objectKey, String toKey) {
        try {
            String localExtractPath = System.getProperty("local_extract_path",
                    System.getenv().getOrDefault("local_extract_path", "/tmp/extract"));
            download(container, objectKey, localExtractPath, false);
            String archiveName = objectKey.contains("/")
                    ? objectKey.substring(objectKey.lastIndexOf('/') + 1)
                    : objectKey;
            String folderName = toKey.contains("/")
                    ? toKey.substring(toKey.lastIndexOf('/') + 1)
                    : toKey;
            String localFolder = localExtractPath + "/" + folderName;
            FileUtil.unZip(localExtractPath + "/" + archiveName, localFolder);
            upload(container, localFolder, toKey, true, 1, 0, null);
        } catch (StorageServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageServiceException("Extract archive failed: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getPaths(String container, List<Blob> objects) {
        String hdfsPrefix = getHdfsPrefix(container);
        List<String> paths = new ArrayList<>();
        for (Blob blob : objects) {
            paths.add(hdfsPrefix + blob.getKey());
        }
        return paths;
    }

    @Override
    public String getUri(String container, String prefix, boolean isDirectory) {
        List<String> keys = listObjectKeys(container, prefix);
        if (keys.isEmpty()) {
            throw new StorageServiceException("The given prefix is incorrect: " + prefix);
        }
        String firstKey = keys.get(0);
        String uri = getObjectUri(container, firstKey);
        if (uri != null && !uri.isEmpty()) {
            int idx = uri.indexOf(prefix);
            if (idx >= 0) {
                return uri.substring(0, idx) + prefix;
            }
            return uri;
        }
        throw new StorageServiceException("URI not available for the given prefix: " + prefix);
    }
}
