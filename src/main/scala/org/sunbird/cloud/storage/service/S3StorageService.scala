package org.sunbird.cloud.storage.service

import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.factory.StorageConfig

class S3StorageService(config: StorageConfig) extends BaseStorageService {

    var context = ContextBuilder.newBuilder("aws-s3").credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
    var blobStore = context.getBlobStore

    override def getPaths(container: String, objects: List[Blob]): List[String] = {
        objects.map{f => "s3n://" + container + "/" + f.key}
    }
}
