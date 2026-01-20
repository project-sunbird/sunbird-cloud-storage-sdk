package org.sunbird.cloud.storage.service

import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.util.IAMCredentialsSupplier

class AzureStorageService(config: StorageConfig)  extends BaseStorageService  {

    if(IAMCredentialsSupplier.isIAMAuth(config.authType.getOrElse(""))) {
        var context = ContextBuilder.newBuilder("azureblob").credentialsSupplier(new IAMCredentialsSupplier("azure")).buildView(classOf[BlobStoreContext])
    } else {
        var context = ContextBuilder.newBuilder("azureblob").credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
    }
    var blobStore = context.getBlobStore

    override def getPaths(container: String, objects: List[Blob]): List[String] = {
        if(IAMCredentialsSupplier.isIAMAuth(config.authType.getOrElse(""))) {
            objects.map{f => "abfss://" + container + "@" + config.storageKey + ".dfs.core.windows.net/" + f.key}
        } else {
            objects.map{f => "wasb://" + container + "@" + config.storageKey + ".blob.core.windows.net/" + f.key}
        }
    }
}
