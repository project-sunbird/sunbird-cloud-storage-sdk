package org.sunbird.cloud.storage.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class FileUtilTest {

    @TempDir
    Path tempDir;

    @Test
    void listFilesRecursive_returnsEmptyForNonExistentDir() {
        List<File> files = FileUtil.listFilesRecursive(new File("/nonexistent/path"));
        assertTrue(files.isEmpty());
    }

    @Test
    void listFilesRecursive_returnsSingleFile() throws IOException {
        File f = tempDir.resolve("file.txt").toFile();
        f.createNewFile();
        List<File> files = FileUtil.listFilesRecursive(f);
        assertEquals(1, files.size());
        assertEquals(f, files.get(0));
    }

    @Test
    void listFilesRecursive_recursesSubdirectories() throws IOException {
        Path sub = tempDir.resolve("sub");
        Files.createDirectory(sub);
        Files.writeString(tempDir.resolve("root.txt"), "root");
        Files.writeString(sub.resolve("child.txt"), "child");

        List<File> files = FileUtil.listFilesRecursive(tempDir.toFile());
        assertEquals(2, files.size());
        assertTrue(files.stream().anyMatch(f -> f.getName().equals("root.txt")));
        assertTrue(files.stream().anyMatch(f -> f.getName().equals("child.txt")));
    }

    @Test
    void listFilesRecursive_ignoresDirectoriesThemselves() throws IOException {
        Path sub = Files.createDirectory(tempDir.resolve("emptydir"));
        // directory exists but no files
        List<File> files = FileUtil.listFilesRecursive(tempDir.toFile());
        assertTrue(files.isEmpty());
    }

    @Test
    void copyStream_writesFileToDestination() throws IOException {
        String content = "hello cloud storage";
        InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        String destDir = tempDir.toString() + "/";

        FileUtil.copyStream(is, destDir, "output.txt");

        Path output = tempDir.resolve("output.txt");
        assertTrue(Files.exists(output));
        assertEquals(content, Files.readString(output));
    }

    @Test
    void copyStream_createsDirectoryIfAbsent() throws IOException {
        String content = "test content";
        InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        String newDir = tempDir.resolve("newsubdir").toString() + "/";

        FileUtil.copyStream(is, newDir, "file.txt");

        assertTrue(Files.exists(Path.of(newDir, "file.txt")));
    }

    @Test
    void unZip_extractsFilesCorrectly() throws IOException {
        // Create a test zip
        Path zipFile = tempDir.resolve("test.zip");
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile.toFile()))) {
            zos.putNextEntry(new ZipEntry("data/file1.txt"));
            zos.write("content1".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("data/file2.txt"));
            zos.write("content2".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        Path outputDir = tempDir.resolve("extracted");
        FileUtil.unZip(zipFile.toString(), outputDir.toString());

        assertTrue(Files.exists(outputDir.resolve("data/file1.txt")));
        assertTrue(Files.exists(outputDir.resolve("data/file2.txt")));
        assertEquals("content1", Files.readString(outputDir.resolve("data/file1.txt")));
    }

    @Test
    void unZip_skipsHiddenAndUnderscoreFiles() throws IOException {
        Path zipFile = tempDir.resolve("test.zip");
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile.toFile()))) {
            zos.putNextEntry(new ZipEntry(".hidden"));
            zos.write("hidden".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("_metadata"));
            zos.write("meta".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("real.txt"));
            zos.write("real".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        Path outputDir = tempDir.resolve("out");
        FileUtil.unZip(zipFile.toString(), outputDir.toString());

        assertFalse(Files.exists(outputDir.resolve(".hidden")));
        assertFalse(Files.exists(outputDir.resolve("_metadata")));
        assertTrue(Files.exists(outputDir.resolve("real.txt")));
    }
}
