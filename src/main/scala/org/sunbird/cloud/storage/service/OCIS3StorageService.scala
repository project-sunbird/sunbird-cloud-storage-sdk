package org.sunbird.cloud.storage.service

import com.google.common.io.Files
import com.google.common.hash
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext

import org.sunbird.cloud.storage.{BaseStorageService, Model}
import org.sunbird.cloud.storage.factory.StorageConfig

import java.io.File
import java.util.Properties

class OCIS3StorageService (config: StorageConfig) extends BaseStorageService {


  val overrides = new Properties()

  overrides.setProperty("jclouds.provider", "s3")
  overrides.setProperty("jclouds.endpoint", config.endPoint.get)
  overrides.setProperty("jclouds.s3.virtual-host-buckets", "false")
  overrides.setProperty("jclouds.strip-expect-header", "true")
  overrides.setProperty("jclouds.regions", config.region.get)
  overrides.setProperty("jclouds.s3.signer-version", "4")

  var context = ContextBuilder.newBuilder("aws-s3")
    .credentials(config.storageKey, config.storageSecret)
    .overrides(overrides)
    .endpoint(config.endPoint.get).buildView(classOf[BlobStoreContext])
  var blobStore = context.getBlobStore

  /**
   * Get HDFS compatible file paths to be used in tech stack like Spark.
   * For ex: for S3 the file path is prefixed with s3n://<bucket>/<key> and for Azure blob storage it would be wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<key>/
   *
   * @param container String - The container/bucket of the objects
   * @param objects   List[Blob] - The Blob objects in the given container
   * @return List[String] - HDFS compatible file paths.
   */
  override def getPaths(container: String, objects: List[Model.Blob]): List[String] = {
    objects.map{f => "s3n://" + container + "/" + f.key}
  }

  override def createContainerInLocation(container: String): Unit = {
    if (!( blobStore.containerExists(container))) {
      blobStore.createContainerInLocation(null, container)
    }
  }

  override def putBlob(objectKey: String, file: File, container: String): Unit = {
    val payload = Files.asByteSource(file)
    val payloadSize  = payload.size()
    val payloadMD5 = payload.hash(hash.Hashing.md5())
    val  contentType = tika.detect(file)
    val blob = blobStore.blobBuilder(objectKey).payload(payload).contentType(contentType).contentEncoding("UTF-8").contentLength(payloadSize).contentMD5(payloadMD5).build()
    blobStore.putBlob(container, blob)
  }

}
