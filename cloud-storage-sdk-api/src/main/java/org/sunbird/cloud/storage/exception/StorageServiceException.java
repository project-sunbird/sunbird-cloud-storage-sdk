package org.sunbird.cloud.storage.exception;

/**
 * Runtime exception for cloud storage operations.
 */
public class StorageServiceException extends RuntimeException {

    public StorageServiceException(String message) {
        super(message);
    }

    public StorageServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
