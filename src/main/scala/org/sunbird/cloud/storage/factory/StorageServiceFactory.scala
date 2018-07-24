package org.sunbird.cloud.storage.factory

import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.service.{AzureStorageService, S3StorageService}

case class StorageConfig(`type`: String, storageKey: String, storageSecret: String)

object StorageServiceFactory {

    def getStorageService(config: StorageConfig): BaseStorageService = {
        config.`type`.toLowerCase() match {
            case "s3"      =>
                new S3StorageService(config);
            case "azure"   =>
                new AzureStorageService(config);
            case _         =>
                throw new StorageServiceException("Unknown storage type found");
        }
    }
}
