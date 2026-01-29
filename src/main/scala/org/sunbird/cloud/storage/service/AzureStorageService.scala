package org.sunbird.cloud.storage.service

import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.util.{IAMCredentialsSupplier, CommonUtil}

class AzureStorageService(config: StorageConfig)  extends BaseStorageService  {

    var context = if(CommonUtil.isIAMAuth(config.authType)) {
        ContextBuilder.newBuilder("azureblob").credentialsSupplier(new IAMCredentialsSupplier("azure")).buildView(classOf[BlobStoreContext])
    } else {
        ContextBuilder.newBuilder("azureblob").credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
    }
    var blobStore = context.getBlobStore

    override def getPaths(container: String, objects: List[Blob]): List[String] = {
        if(CommonUtil.isIAMAuth(config.authType)) {
            objects.map{f => "abfss://" + container + "@" + config.storageKey + ".dfs.core.windows.net/" + f.key}
        } else {
            objects.map{f => "wasb://" + container + "@" + config.storageKey + ".blob.core.windows.net/" + f.key}
        }
    }
}
