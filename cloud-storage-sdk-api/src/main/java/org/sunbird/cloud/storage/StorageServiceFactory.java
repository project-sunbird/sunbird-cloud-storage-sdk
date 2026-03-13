package org.sunbird.cloud.storage;

import org.sunbird.cloud.storage.exception.StorageServiceException;

import java.util.ServiceLoader;

/**
 * Factory for creating {@link IStorageService} instances.
 * Uses Java {@link ServiceLoader} to discover CSP-specific implementations at runtime.
 *
 * <p>Consumers add the appropriate CSP module JAR to their classpath and the factory
 * automatically discovers and uses the matching provider.</p>
 */
public final class StorageServiceFactory {

    private StorageServiceFactory() {
    }

    /**
     * Create a storage service for the given configuration.
     * The appropriate provider is discovered via ServiceLoader based on the storage type.
     *
     * @param config the storage configuration
     * @return a new storage service instance
     * @throws StorageServiceException if no provider is found for the given type
     */
    public static IStorageService getStorageService(StorageConfig config) {
        ServiceLoader<StorageServiceProvider> loader =
                ServiceLoader.load(StorageServiceProvider.class);
        for (StorageServiceProvider provider : loader) {
            if (provider.supports(config.getType())) {
                return provider.create(config);
            }
        }
        throw new StorageServiceException(
                "No StorageService provider found for type: " + config.getType() +
                        ". Ensure the appropriate CSP module JAR is on the classpath.");
    }
}
