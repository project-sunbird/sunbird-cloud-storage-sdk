package org.sunbird.cloud.storage.service.azure;

import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.StorageServiceProvider;

/**
 * SPI provider for Azure Blob Storage.
 */
public class AzureStorageServiceProvider implements StorageServiceProvider {

    @Override
    public boolean supports(StorageConfig.StorageType type) {
        return type == StorageConfig.StorageType.AZURE;
    }

    @Override
    public IStorageService create(StorageConfig config) {
        return new AzureStorageService(config);
    }
}
