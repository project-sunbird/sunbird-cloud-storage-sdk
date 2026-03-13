package org.sunbird.cloud.storage.model;

/**
 * Represents a target for deletion in cloud storage, combining object key with directory flag.
 */
public final class DeleteTarget {

    private final String objectKey;
    private final boolean directory;

    public DeleteTarget(String objectKey, boolean directory) {
        this.objectKey = objectKey;
        this.directory = directory;
    }

    public static DeleteTarget file(String objectKey) {
        return new DeleteTarget(objectKey, false);
    }

    public static DeleteTarget directory(String objectKey) {
        return new DeleteTarget(objectKey, true);
    }

    public String getObjectKey() {
        return objectKey;
    }

    public boolean isDirectory() {
        return directory;
    }

    @Override
    public String toString() {
        return "DeleteTarget{key='" + objectKey + "', directory=" + directory + "}";
    }
}
