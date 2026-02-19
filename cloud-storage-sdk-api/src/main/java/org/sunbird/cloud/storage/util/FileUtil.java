package org.sunbird.cloud.storage.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * File utility methods for recursive listing, stream copy, and zip extraction.
 */
public final class FileUtil {

    private FileUtil() {
    }

    /**
     * Recursively list all files under the given directory.
     *
     * @param file the directory or file to list
     * @return list of files (not directories)
     */
    public static List<File> listFilesRecursive(File file) {
        List<File> result = new ArrayList<>();
        if (file.exists() && file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    if (child.isDirectory()) {
                        result.addAll(listFilesRecursive(child));
                    } else {
                        result.add(child);
                    }
                }
            }
        } else if (file.exists() && file.isFile()) {
            result.add(file);
        }
        return result;
    }

    /**
     * Copy an input stream to a file at the given path.
     *
     * @param from     the source input stream
     * @param path     the destination directory
     * @param fileName the destination file name
     * @throws IOException if an I/O error occurs
     */
    public static void copyStream(InputStream from, String path, String fileName) throws IOException {
        Files.createDirectories(Paths.get(path));
        Files.copy(from, Paths.get(path, fileName), StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Extract a ZIP archive to the specified output folder.
     * Skips hidden files (starting with '.'), underscore-prefixed files, and blank file names.
     *
     * @param zipFile      path to the ZIP file
     * @param outputFolder path to the output directory
     * @throws IOException if an I/O error occurs
     */
    public static void unZip(String zipFile, String outputFolder) throws IOException {
        File folder = new File(outputFolder);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        byte[] buffer = new byte[4096];
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                String simpleName = Paths.get(entryName).getFileName().toString();

                if (simpleName.startsWith(".") || simpleName.startsWith("_") || simpleName.isEmpty()) {
                    continue;
                }

                File newFile = new File(outputFolder + File.separator + entryName);
                File parent = newFile.getParentFile();
                if (parent != null && !parent.exists()) {
                    parent.mkdirs();
                }

                try (FileOutputStream fos = new FileOutputStream(newFile)) {
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                }
            }
        }
    }
}
