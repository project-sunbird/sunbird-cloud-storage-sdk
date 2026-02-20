package org.sunbird.cloud.storage;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.sunbird.cloud.storage.model.Blob;
import org.sunbird.cloud.storage.model.DeleteTarget;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base class for CSP integration tests.
 *
 * <p>Subclasses must override {@link #createService()} to provide a configured
 * {@link IStorageService} and {@link #getContainer()} to return a pre-existing
 * writable bucket/container name.</p>
 *
 * <p>Tests are organised to be self-cleaning: each creates objects under a unique
 * per-run prefix and removes them in {@code @AfterAll}.</p>
 */
public abstract class BaseStorageServiceIntegrationTest {

    @TempDir
    protected static Path tempDir;

    protected static IStorageService service;

    /** Unique prefix for this test run to avoid collisions. */
    protected static String testPrefix;

    /** Return a configured storage service. Called once before all tests. */
    protected abstract IStorageService createService();

    /** Return the name of a pre-existing, writable container/bucket. */
    protected abstract String getContainer();

    @BeforeAll
    void setUpService() {
        service = createService();
        testPrefix = "sdk-integration-test/" + UUID.randomUUID() + "/";
    }

    @AfterAll
    void tearDown() {
        if (service != null) {
            try {
                // Best-effort cleanup: delete everything under the test prefix
                List<String> keys = service.listObjectKeys(getContainer(), testPrefix);
                if (!keys.isEmpty()) {
                    service.deleteObjects(getContainer(),
                            keys.stream()
                                    .map(DeleteTarget::file)
                                    .toList());
                }
            } finally {
                service.close();
            }
        }
    }

    // ── Upload & list ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("Upload single file and verify it appears in listing")
    void uploadAndList() throws IOException {
        Path file = tempDir.resolve("upload-test.txt");
        Files.writeString(file, "integration test content");
        String key = testPrefix + "upload-test.txt";

        String url = service.upload(getContainer(), file.toString(), key);

        assertNotNull(url, "Upload should return a URL");
        assertFalse(url.isBlank());
        List<String> keys = service.listObjectKeys(getContainer(), testPrefix);
        assertTrue(keys.contains(key), "Listed keys should contain the uploaded key");
    }

    @Test
    @DisplayName("Upload folder recursively")
    void uploadFolder() throws IOException {
        Path dir = tempDir.resolve("upload-dir");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("a.txt"), "aaa");
        Files.writeString(dir.resolve("b.txt"), "bbb");
        String folderPrefix = testPrefix + "folder/";

        service.upload(getContainer(), dir.toString(), folderPrefix, true, 1, 0, null);

        List<String> keys = service.listObjectKeys(getContainer(), folderPrefix);
        assertTrue(keys.size() >= 2);
    }

    // ── put() (bytes) ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("Put bytes directly and verify retrieval")
    void putAndGetData() {
        String key = testPrefix + "bytes-object.txt";
        byte[] content = "line1\nline2\nline3".getBytes(StandardCharsets.UTF_8);

        service.put(getContainer(), content, key);

        List<String> lines = service.getObjectData(getContainer(), key);
        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line3", lines.get(2));
    }

    // ── getObject / metadata ──────────────────────────────────────────────────

    @Test
    @DisplayName("Get object metadata")
    void getObjectMetadata() throws IOException {
        Path file = tempDir.resolve("meta-test.txt");
        String content = "metadata test";
        Files.writeString(file, content);
        String key = testPrefix + "meta-test.txt";
        service.upload(getContainer(), file.toString(), key);

        Blob blob = service.getObject(getContainer(), key);

        assertEquals(key, blob.getKey());
        assertEquals(content.getBytes().length, blob.getContentLength());
        assertNotNull(blob.getLastModified());
    }

    @Test
    @DisplayName("Get object with payload")
    void getObjectWithPayload() throws IOException {
        Path file = tempDir.resolve("payload-test.txt");
        Files.writeString(file, "payload data");
        String key = testPrefix + "payload-test.txt";
        service.upload(getContainer(), file.toString(), key);

        Blob blob = service.getObject(getContainer(), key, true);

        assertNotNull(blob.getPayload(), "Payload should be present");
        assertTrue(blob.getPayload().length > 0);
    }

    // ── Download ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Download file and verify content")
    void downloadFile() throws IOException {
        String expected = "download me";
        String key = testPrefix + "download-test.txt";
        service.put(getContainer(), expected.getBytes(StandardCharsets.UTF_8), key);

        Path downloadDir = tempDir.resolve("downloads");
        Files.createDirectories(downloadDir);
        service.download(getContainer(), key, downloadDir.toString() + "/");

        Path downloaded = downloadDir.resolve("download-test.txt");
        assertTrue(Files.exists(downloaded));
        assertEquals(expected, Files.readString(downloaded));
    }

    // ── Signed URLs ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("Generate read signed URL — URL must be non-blank")
    void getSignedReadUrl() throws IOException {
        Path file = tempDir.resolve("signed-read.txt");
        Files.writeString(file, "signed url test");
        String key = testPrefix + "signed-read.txt";
        service.upload(getContainer(), file.toString(), key);

        String url = service.getSignedURL(getContainer(), key, 3600, "r");

        assertNotNull(url);
        assertFalse(url.isBlank());
        // URL should reference the container and key in some form
        assertTrue(url.contains(getContainer()) || url.contains(key) || url.startsWith("https://"),
                "Signed URL should be a valid HTTPS URL");
    }

    @Test
    @DisplayName("Generate write signed URL — URL must be non-blank")
    void getSignedWriteUrl() {
        String key = testPrefix + "signed-write.txt";
        String url = service.getSignedURL(getContainer(), key, 3600, "w");

        assertNotNull(url);
        assertFalse(url.isBlank());
    }

    @Test
    @DisplayName("getSignedURLV2 — supports content type and additional params")
    void getSignedUrlV2() throws IOException {
        Path file = tempDir.resolve("signed-v2.txt");
        Files.writeString(file, "v2 test");
        String key = testPrefix + "signed-v2.txt";
        service.upload(getContainer(), file.toString(), key);

        String url = service.getSignedURLV2(getContainer(), key, 3600, "r",
                "text/plain", null);

        assertNotNull(url);
        assertFalse(url.isBlank());
    }

    // ── Copy ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Copy single object within the same container")
    void copyObject() throws IOException {
        Path file = tempDir.resolve("copy-src.txt");
        Files.writeString(file, "copy source");
        String srcKey = testPrefix + "copy-src.txt";
        String dstKey = testPrefix + "copy-dst.txt";
        service.upload(getContainer(), file.toString(), srcKey);

        service.copyObjects(getContainer(), srcKey, getContainer(), dstKey);

        List<String> keys = service.listObjectKeys(getContainer(), testPrefix);
        assertTrue(keys.contains(srcKey), "Source key should still exist");
        assertTrue(keys.contains(dstKey), "Destination key should exist");
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Delete object and verify removal")
    void deleteObject() throws IOException {
        Path file = tempDir.resolve("delete-test.txt");
        Files.writeString(file, "to be deleted");
        String key = testPrefix + "delete-test.txt";
        service.upload(getContainer(), file.toString(), key);
        assertTrue(service.listObjectKeys(getContainer(), key).contains(key));

        service.deleteObject(getContainer(), key);

        List<String> keys = service.listObjectKeys(getContainer(), key);
        assertFalse(keys.contains(key), "Deleted key should not appear in listing");
    }

    // ── Search ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Search objects by date prefix")
    void searchObjectsByDate() {
        String datePrefix = testPrefix + "dated/";
        service.put(getContainer(), "d1".getBytes(), datePrefix + "2024-06-01/file.txt");
        service.put(getContainer(), "d2".getBytes(), datePrefix + "2024-06-02/file.txt");
        service.put(getContainer(), "d3".getBytes(), datePrefix + "2024-06-10/file.txt");

        List<Blob> results = service.searchObjects(
                getContainer(), datePrefix, "2024-06-01", "2024-06-02", null, "yyyy-MM-dd");

        assertEquals(2, results.size());
    }

    @Test
    @DisplayName("Search object keys by delta")
    void searchObjectKeysByDelta() {
        String datePrefix = testPrefix + "delta/";
        service.put(getContainer(), "d1".getBytes(), datePrefix + "2024-07-08/file.txt");
        service.put(getContainer(), "d2".getBytes(), datePrefix + "2024-07-09/file.txt");
        service.put(getContainer(), "d3".getBytes(), datePrefix + "2024-07-10/file.txt");
        service.put(getContainer(), "d4".getBytes(), datePrefix + "2024-07-20/file.txt");

        List<String> keys = service.searchObjectKeys(
                getContainer(), datePrefix, null, "2024-07-10", 2, "yyyy-MM-dd");

        assertEquals(3, keys.size());
    }

    // ── HDFS paths ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("getPaths returns HDFS-prefixed paths")
    void getPaths() {
        Blob blob = new Blob(testPrefix + "some-file.txt", 100, null, java.util.Map.of());
        List<String> paths = service.getPaths(getContainer(), List.of(blob));

        assertEquals(1, paths.size());
        String path = paths.get(0);
        assertTrue(path.contains(getContainer()), "Path should contain the container name");
        assertTrue(path.endsWith(testPrefix + "some-file.txt"),
                "Path should end with the blob key");
    }
}
