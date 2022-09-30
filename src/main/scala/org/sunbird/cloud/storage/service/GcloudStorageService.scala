package org.sunbird.cloud.storage.service

import com.google.common.io.Files
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.factory.StorageConfig

import java.io.File

class GcloudStorageService(config: StorageConfig) extends BaseStorageService  {

  var context = ContextBuilder.newBuilder("google-cloud-storage").credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
  var blobStore = context.getBlobStore

  override def getPaths(container: String, objects: List[Blob]): List[String] = {
    objects.map{f => "gs://" + container + "/" + f.key}
  }

  // Overriding upload method since multipart upload not working for GCP
  override def upload(container: String, file: String, objectKey: String, isDirectory: Option[Boolean] = Option(false), attempt: Option[Int] = Option(1), retryCount: Option[Int] = None, ttl: Option[Int] = None): String = {
    try {
      if(isDirectory.get) {
        val d = new File(file)
        val files = filesList(d)
        val list = files.map {f =>
          val key = objectKey + f.getAbsolutePath.split(d.getAbsolutePath + File.separator).last
          upload(container, f.getAbsolutePath, key, Option(false), attempt, retryCount, ttl)
        }
        list.mkString(",")
      }
      else {
        if (attempt.getOrElse(1) >= retryCount.getOrElse(maxRetries)) {
          val message = s"Failed to upload. file: $file, key: $objectKey, attempt: $attempt, maxAttempts: $retryCount. Exceeded maximum number of retries"
          throw new StorageServiceException(message)
        }

        blobStore.createContainerInLocation(null, container)
        val fileObj = new File(file)
        val payload = Files.asByteSource(fileObj)
        val  contentType = tika.detect(fileObj)
        val blob = blobStore.blobBuilder(objectKey).payload(payload).contentType(contentType).contentEncoding("UTF-8").contentLength(payload.size()).build()
        blobStore.putBlob(container, blob)
        if (ttl.isDefined) {
          getPutSignedURL(container, objectKey, Option(ttl.get), None, Option(contentType))
        } else {
          val host = "https://storage.googleapis.com/"
          val name = blobStore.blobMetadata(container, objectKey).getName
          host + container + "/" + name
        }
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Thread.sleep(attempt.getOrElse(1)*2000)
        val uploadAttempt = attempt.getOrElse(1) + 1
        if (uploadAttempt <= retryCount.getOrElse(maxRetries)) {
          upload(container, file, objectKey, isDirectory, Option(uploadAttempt), retryCount, ttl)
        } else {
          throw e;
        }
      }
    }
  }

  override def getUri(container: String, _prefix: String, isDirectory: Option[Boolean] = Option(false)): String = {
    val keys = listObjectKeys(container, _prefix);
    if (keys.isEmpty)
      throw new StorageServiceException("The given _prefix is incorrect: " + _prefix)
    val prefix = keys.head
    val blob = getObject(container, prefix, Option(false))
    val uri = blob.metadata.get("publicUri")
    if (!uri.isEmpty) {
      if(isDirectory.get){
        throw new StorageServiceException("getUri for directory is not supported for GCP. The given _prefix is incorrect: " + _prefix)
      }
      else{
        val host = "https://storage.googleapis.com/"
        val name = blob.metadata.get("name").get.asInstanceOf[String]
        host + container + "/" + name
      }
    } else
      throw new StorageServiceException("uri not available for the given prefix: "+ _prefix)
  }
}