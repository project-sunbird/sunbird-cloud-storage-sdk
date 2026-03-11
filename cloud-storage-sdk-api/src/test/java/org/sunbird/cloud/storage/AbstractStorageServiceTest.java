package org.sunbird.cloud.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sunbird.cloud.storage.exception.StorageServiceException;
import org.sunbird.cloud.storage.model.Blob;
import org.sunbird.cloud.storage.model.DeleteTarget;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AbstractStorageService orchestration logic using an in-memory
 * storage implementation — no cloud credentials required.
 */
class AbstractStorageServiceTest {

    @TempDir
    Path tempDir;

    private InMemoryStorageService service;

    @BeforeEach
    void setUp() {
        service = new InMemoryStorageService();
    }

    // ── Upload / Put ──────────────────────────────────────────────────────────

    @Test
    void upload_singleFile_returnsUri() throws IOException {
        Path file = tempDir.resolve("file.txt");
        Files.writeString(file, "hello");

        String uri = service.upload("bucket", file.toString(), "prefix/file.txt");

        assertEquals("mem://bucket/prefix/file.txt", uri);
        assertTrue(service.exists("bucket", "prefix/file.txt"));
    }

    @Test
    void upload_directory_uploadsAllFiles() throws IOException {
        Path dir = tempDir.resolve("data");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("a.txt"), "aaa");
        Files.writeString(dir.resolve("b.txt"), "bbb");

        String result = service.upload("bucket", dir.toString(), "prefix/data", true, 1, 0, null);

        assertTrue(result.contains("prefix/data/a.txt"));
        assertTrue(result.contains("prefix/data/b.txt"));
        assertTrue(service.exists("bucket", "prefix/data/a.txt"));
        assertTrue(service.exists("bucket", "prefix/data/b.txt"));
    }

    @Test
    void upload_directory_withTrailingSlash_noDoubleSlash() throws IOException {
        Path dir = tempDir.resolve("data-slash");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("a.txt"), "aaa");
        Files.writeString(dir.resolve("b.txt"), "bbb");

        // objectKey already ends with '/' — must not produce "prefix/data//a.txt"
        String result = service.upload("bucket", dir.toString(), "prefix/data/", true, 1, 0, null);

        assertTrue(result.contains("prefix/data/a.txt"), "Expected prefix/data/a.txt, got: " + result);
        assertTrue(result.contains("prefix/data/b.txt"), "Expected prefix/data/b.txt, got: " + result);
        assertFalse(result.contains("//"), "Keys must not contain double slashes: " + result);
        assertTrue(service.exists("bucket", "prefix/data/a.txt"));
        assertTrue(service.exists("bucket", "prefix/data/b.txt"));
    }

    @Test
    void upload_directory_withoutTrailingSlash_addsSingleSlash() throws IOException {
        Path dir = tempDir.resolve("data-noslash");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("c.txt"), "ccc");

        // objectKey has no trailing '/' — must add exactly one
        String result = service.upload("bucket", dir.toString(), "prefix/data", true, 1, 0, null);

        assertTrue(result.contains("prefix/data/c.txt"), "Expected prefix/data/c.txt, got: " + result);
        assertFalse(result.contains("//"), "Keys must not contain double slashes: " + result);
        assertTrue(service.exists("bucket", "prefix/data/c.txt"));
    }

    @Test
    void uploadFolder_withTrailingSlash_noDoubleSlash() throws ExecutionException, InterruptedException {
        Path dir = tempDir.resolve("async-slash");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("x.txt"), "xxx");
        Files.writeString(dir.resolve("y.txt"), "yyy");

        // async uploadFolder — same prefix logic at line 157-158
        List<String> urls = service.uploadFolder("bucket", dir.toString(), "async/data/")
                .get();

        assertTrue(urls.stream().anyMatch(u -> u.contains("async/data/x.txt")),
                "Expected async/data/x.txt in: " + urls);
        assertTrue(urls.stream().anyMatch(u -> u.contains("async/data/y.txt")),
                "Expected async/data/y.txt in: " + urls);
        urls.forEach(u -> assertFalse(u.contains("//"),
                "URL must not contain double slashes: " + u));
    }

    @Test
    void uploadFolder_withoutTrailingSlash_addsSingleSlash() throws ExecutionException, InterruptedException {
        Path dir = tempDir.resolve("async-noslash");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("z.txt"), "zzz");

        List<String> urls = service.uploadFolder("bucket", dir.toString(), "async/data")
                .get();

        assertTrue(urls.stream().anyMatch(u -> u.contains("async/data/z.txt")),
                "Expected async/data/z.txt in: " + urls);
        urls.forEach(u -> assertFalse(u.contains("//"),
                "URL must not contain double slashes: " + u));
    }

    @Test
    void upload_exceedsMaxRetries_throwsException() {
        // Non-existent file forces an exception, and retry count 1 means fail immediately
        assertThrows(StorageServiceException.class, () ->
                service.upload("bucket", "/no/such/file.txt", "key", false, 1, 1, null));
    }

    @Test
    void put_bytesContent_returnsUri() {
        byte[] content = "data bytes".getBytes(StandardCharsets.UTF_8);
        String uri = service.put("bucket", content, "obj/data.bin");

        assertEquals("mem://bucket/obj/data.bin", uri);
        assertTrue(service.exists("bucket", "obj/data.bin"));
    }

    // ── List / Get ────────────────────────────────────────────────────────────

    @Test
    void listObjectKeys_returnsMatchingKeys() {
        service.putDirect("bucket", "logs/2024-01-01.txt", "a".getBytes());
        service.putDirect("bucket", "logs/2024-01-02.txt", "b".getBytes());
        service.putDirect("bucket", "other/file.txt", "c".getBytes());

        List<String> keys = service.listObjectKeys("bucket", "logs/");

        assertEquals(2, keys.size());
        assertTrue(keys.contains("logs/2024-01-01.txt"));
        assertTrue(keys.contains("logs/2024-01-02.txt"));
    }

    @Test
    void listObjects_returnsBlobs() {
        service.putDirect("bucket", "data/file.txt", "content".getBytes());

        List<Blob> blobs = service.listObjects("bucket", "data/");

        assertEquals(1, blobs.size());
        assertEquals("data/file.txt", blobs.get(0).getKey());
    }

    @Test
    void getObject_returnsMetadata() {
        service.putDirect("bucket", "key.txt", "hello".getBytes());

        Blob blob = service.getObject("bucket", "key.txt");

        assertEquals("key.txt", blob.getKey());
        assertEquals(5L, blob.getContentLength());
    }

    @Test
    void getObject_withPayload_returnsBytes() {
        service.putDirect("bucket", "data.txt", "payload".getBytes(StandardCharsets.UTF_8));

        Blob blob = service.getObject("bucket", "data.txt", true);

        assertNotNull(blob.getPayload());
        assertEquals("payload", new String(blob.getPayload(), StandardCharsets.UTF_8));
    }

    @Test
    void getObjectData_returnsLines() {
        service.putDirect("bucket", "lines.txt", "line1\nline2\nline3".getBytes());

        List<String> lines = service.getObjectData("bucket", "lines.txt");

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    @Test
    void deleteObject_removesFile() {
        service.putDirect("bucket", "delete-me.txt", "x".getBytes());
        assertTrue(service.exists("bucket", "delete-me.txt"));

        service.deleteObject("bucket", "delete-me.txt");

        assertFalse(service.exists("bucket", "delete-me.txt"));
    }

    @Test
    void deleteObjects_directory_removesAllKeysWithPrefix() {
        service.putDirect("bucket", "dir/a.txt", "a".getBytes());
        service.putDirect("bucket", "dir/b.txt", "b".getBytes());
        service.putDirect("bucket", "other.txt", "o".getBytes());

        service.deleteObjects("bucket", List.of(DeleteTarget.directory("dir/")));

        assertFalse(service.exists("bucket", "dir/a.txt"));
        assertFalse(service.exists("bucket", "dir/b.txt"));
        assertTrue(service.exists("bucket", "other.txt"));
    }

    @Test
    void deleteObjects_multipleTargets() {
        service.putDirect("bucket", "a.txt", "a".getBytes());
        service.putDirect("bucket", "b.txt", "b".getBytes());

        service.deleteObjects("bucket", List.of(
                DeleteTarget.file("a.txt"),
                DeleteTarget.file("b.txt")));

        assertFalse(service.exists("bucket", "a.txt"));
        assertFalse(service.exists("bucket", "b.txt"));
    }

    // ── Copy ─────────────────────────────────────────────────────────────────

    @Test
    void copyObjects_singleFile_copiesCorrectly() {
        service.putDirect("src-bucket", "original.txt", "data".getBytes());

        service.copyObjects("src-bucket", "original.txt", "dst-bucket", "copy.txt");

        assertTrue(service.exists("dst-bucket", "copy.txt"));
        assertTrue(service.exists("src-bucket", "original.txt")); // original still there
    }

    @Test
    void copyObjects_directory_copiesAllFiles() {
        service.putDirect("src", "folder/x.txt", "x".getBytes());
        service.putDirect("src", "folder/y.txt", "y".getBytes());

        service.copyObjects("src", "folder/", "dst", "backup/", true);

        assertTrue(service.exists("dst", "backup/x.txt"));
        assertTrue(service.exists("dst", "backup/y.txt"));
    }

    // ── Search ────────────────────────────────────────────────────────────────

    @Test
    void searchObjects_withDateRange_returnsMatchingBlobs() {
        service.putDirect("bucket", "logs/2024-03-01/data.txt", "a".getBytes());
        service.putDirect("bucket", "logs/2024-03-02/data.txt", "b".getBytes());
        service.putDirect("bucket", "logs/2024-03-03/data.txt", "c".getBytes());
        service.putDirect("bucket", "logs/2024-04-01/data.txt", "d".getBytes());

        List<Blob> results = service.searchObjects(
                "bucket", "logs/", "2024-03-01", "2024-03-02", null, "yyyy-MM-dd");

        assertEquals(2, results.size());
    }

    @Test
    void searchObjectKeys_withDelta_returnsKeys() {
        service.putDirect("bucket", "events/2024-05-08/file.txt", "x".getBytes());
        service.putDirect("bucket", "events/2024-05-09/file.txt", "y".getBytes());
        service.putDirect("bucket", "events/2024-05-10/file.txt", "z".getBytes());

        List<String> keys = service.searchObjectKeys(
                "bucket", "events/", null, "2024-05-10", 2, "yyyy-MM-dd");

        // delta=2 from 2024-05-10 means from 2024-05-08 to 2024-05-10 (inclusive)
        assertEquals(3, keys.size());
    }

    @Test
    void searchObjects_noDatesOrDelta_listsAllWithPrefix() {
        service.putDirect("bucket", "prefix/a.txt", "a".getBytes());
        service.putDirect("bucket", "prefix/b.txt", "b".getBytes());
        service.putDirect("bucket", "other/c.txt", "c".getBytes());

        List<Blob> results = service.searchObjects(
                "bucket", "prefix/", null, null, null, "yyyy-MM-dd");

        assertEquals(2, results.size());
    }

    // ── URI / Paths ───────────────────────────────────────────────────────────

    @Test
    void getPaths_returnsHdfsPrefixedPaths() {
        Blob b1 = new Blob("key1.txt", 10, new Date(), Map.of());
        Blob b2 = new Blob("key2.txt", 20, new Date(), Map.of());

        List<String> paths = service.getPaths("bucket", List.of(b1, b2));

        assertEquals(List.of("mem://bucket/key1.txt", "mem://bucket/key2.txt"), paths);
    }

    @Test
    void getUri_returnsBaseUri() {
        service.putDirect("bucket", "prefix/file.txt", "x".getBytes());

        String uri = service.getUri("bucket", "prefix/");

        assertEquals("mem://bucket/prefix/", uri);
    }

    @Test
    void getUri_emptyPrefix_throwsException() {
        assertThrows(StorageServiceException.class, () ->
                service.getUri("bucket", "nonexistent/prefix/"));
    }

    // ── Download ──────────────────────────────────────────────────────────────

    @Test
    void download_singleFile_writesToLocalPath() throws IOException {
        service.putDirect("bucket", "remote/data.txt", "file-content".getBytes());
        String localDir = tempDir.toString() + "/";

        service.download("bucket", "remote/data.txt", localDir);

        Path downloaded = tempDir.resolve("data.txt");
        assertTrue(Files.exists(downloaded));
        assertEquals("file-content", Files.readString(downloaded));
    }

    @Test
    void download_directory_downloadsAllFiles() throws IOException {
        service.putDirect("bucket", "dir/sub/a.txt", "aaa".getBytes());
        service.putDirect("bucket", "dir/sub/b.txt", "bbb".getBytes());
        String localDir = tempDir.toString() + "/";

        service.download("bucket", "dir/", localDir, true);

        // Files should appear relative to localDir
        assertTrue(Files.exists(tempDir.resolve("sub/a.txt")) ||
                Files.exists(tempDir.resolve("a.txt")));
    }

    // ── Signed URLs ───────────────────────────────────────────────────────────

    @Test
    void getSignedURL_read_returnsUrl() {
        String url = service.getSignedURL("bucket", "key.txt", 3600, "r");
        assertEquals("mem://signed/r/bucket/key.txt?ttl=3600", url);
    }

    @Test
    void getSignedURL_write_returnsUrl() {
        String url = service.getSignedURL("bucket", "key.txt", 3600, "w");
        assertEquals("mem://signed/w/bucket/key.txt?ttl=3600", url);
    }

    @Test
    void getSignedURLV2_defaultsToRead() {
        String url = service.getSignedURLV2("bucket", "key.txt");
        assertTrue(url.contains("bucket") && url.contains("key.txt"));
    }

    // ── Inner in-memory implementation ───────────────────────────────────────

    static class InMemoryStorageService extends AbstractStorageService {

        /** container -> (key -> bytes) */
        private final Map<String, Map<String, byte[]>> store = new HashMap<>();

        boolean exists(String container, String key) {
            return store.containsKey(container) && store.get(container).containsKey(key);
        }

        void putDirect(String container, String key, byte[] bytes) {
            store.computeIfAbsent(container, k -> new HashMap<>()).put(key, bytes);
        }

        @Override
        protected void ensureContainerExists(String container) {
            store.computeIfAbsent(container, k -> new HashMap<>());
        }

        @Override
        protected String putObject(String container, String objectKey, java.io.File file) {
            try {
                byte[] bytes = Files.readAllBytes(file.toPath());
                store.computeIfAbsent(container, k -> new HashMap<>()).put(objectKey, bytes);
                return "mem://" + container + "/" + objectKey;
            } catch (IOException e) {
                throw new StorageServiceException("Failed to put from file: " + e.getMessage(), e);
            }
        }

        @Override
        protected String putObject(String container, String objectKey, byte[] content) {
            store.computeIfAbsent(container, k -> new HashMap<>()).put(objectKey, content);
            return "mem://" + container + "/" + objectKey;
        }

        @Override
        protected InputStream getObjectStream(String container, String objectKey) {
            Map<String, byte[]> bucket = store.get(container);
            if (bucket == null || !bucket.containsKey(objectKey)) {
                throw new StorageServiceException("Not found: " + container + "/" + objectKey);
            }
            return new ByteArrayInputStream(bucket.get(objectKey));
        }

        @Override
        protected String getObjectUri(String container, String objectKey) {
            return "mem://" + container + "/" + objectKey;
        }

        @Override
        protected BlobDetail getObjectDetail(String container, String objectKey) {
            Map<String, byte[]> bucket = store.get(container);
            if (bucket == null || !bucket.containsKey(objectKey)) {
                throw new StorageServiceException("Not found: " + container + "/" + objectKey);
            }
            byte[] bytes = bucket.get(objectKey);
            Map<String, Object> meta = new HashMap<>();
            meta.put("uri", "mem://" + container + "/" + objectKey);
            return new BlobDetail(objectKey, bytes.length, new Date(), meta,
                    "mem://" + container + "/" + objectKey);
        }

        @Override
        protected List<String> listKeys(String container, String prefix) {
            Map<String, byte[]> bucket = store.getOrDefault(container, Map.of());
            return bucket.keySet().stream()
                    .filter(k -> k.startsWith(prefix))
                    .sorted()
                    .collect(Collectors.toList());
        }

        @Override
        protected void removeObjects(String container, List<String> objectKeys) {
            Map<String, byte[]> bucket = store.get(container);
            if (bucket != null) {
                objectKeys.forEach(bucket::remove);
            }
        }

        @Override
        protected void copyObject(String fromContainer, String fromKey,
                                  String toContainer, String toKey) {
            Map<String, byte[]> src = store.get(fromContainer);
            if (src == null || !src.containsKey(fromKey)) {
                throw new StorageServiceException("Not found: " + fromContainer + "/" + fromKey);
            }
            store.computeIfAbsent(toContainer, k -> new HashMap<>()).put(toKey, src.get(fromKey));
        }

        @Override
        protected String generateSignedGetUrl(String container, String objectKey, int ttlSeconds) {
            return "mem://signed/r/" + container + "/" + objectKey + "?ttl=" + ttlSeconds;
        }

        @Override
        protected String generateSignedPutUrl(String container, String objectKey,
                                              int ttlSeconds, String contentType,
                                              Map<String, String> additionalParams) {
            return "mem://signed/w/" + container + "/" + objectKey + "?ttl=" + ttlSeconds;
        }

        @Override
        protected String getHdfsPrefix(String container) {
            return "mem://" + container + "/";
        }

        @Override
        public void close() {
            store.clear();
        }
    }
}
