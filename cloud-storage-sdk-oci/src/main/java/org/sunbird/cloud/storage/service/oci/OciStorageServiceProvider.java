package org.sunbird.cloud.storage.service.oci;

import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.StorageServiceProvider;

/**
 * SPI provider for Oracle Cloud Infrastructure Object Storage.
 */
public class OciStorageServiceProvider implements StorageServiceProvider {

    @Override
    public boolean supports(StorageConfig.StorageType type) {
        return type == StorageConfig.StorageType.OCI;
    }

    @Override
    public IStorageService create(StorageConfig config) {
        return new OciStorageService(config);
    }
}
