package org.sunbird.cloud.storage.service.gcp;

import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.StorageServiceProvider;

/**
 * SPI provider for Google Cloud Storage.
 */
public class GcpStorageServiceProvider implements StorageServiceProvider {

    @Override
    public boolean supports(StorageConfig.StorageType type) {
        return type == StorageConfig.StorageType.GCLOUD;
    }

    @Override
    public IStorageService create(StorageConfig config) {
        return new GcpStorageService(config);
    }
}
