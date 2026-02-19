package org.sunbird.cloud.storage;

/**
 * Service Provider Interface for cloud storage implementations.
 * Each CSP module provides an implementation registered via
 * {@code META-INF/services/org.sunbird.cloud.storage.StorageServiceProvider}.
 */
public interface StorageServiceProvider {

    /**
     * Check whether this provider supports the given storage type.
     *
     * @param type the storage type
     * @return true if this provider can create a service for the given type
     */
    boolean supports(StorageConfig.StorageType type);

    /**
     * Create a new storage service instance for the given configuration.
     *
     * @param config the storage configuration
     * @return a new storage service instance
     */
    IStorageService create(StorageConfig config);
}
