package org.sunbird.cloud.storage.service.aws;

import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.StorageServiceProvider;

/**
 * SPI provider for AWS S3 and Ceph S3-compatible storage.
 */
public class AwsStorageServiceProvider implements StorageServiceProvider {

    @Override
    public boolean supports(StorageConfig.StorageType type) {
        return type == StorageConfig.StorageType.AWS
                || type == StorageConfig.StorageType.CEPHS3;
    }

    @Override
    public IStorageService create(StorageConfig config) {
        return new AwsStorageService(config);
    }
}
