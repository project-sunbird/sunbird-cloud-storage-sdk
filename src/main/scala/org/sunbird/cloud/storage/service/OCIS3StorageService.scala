package org.sunbird.cloud.storage.service

import com.google.common.collect.ImmutableSet
import com.google.common.io.Files
import com.google.common.hash
import org.jclouds.ContextBuilder
import org.jclouds.aws.s3.blobstore.config.AWSS3BlobStoreContextModule
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.s3.blobstore.config.S3BlobStoreContextModule
import org.sunbird.cloud.storage.exception.StorageServiceException
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
  overrides.setProperty("jclouds.regions", "ap-hyderabad-1")
  overrides.setProperty("jclouds.s3.signer-version", "4")

  val wiring = ImmutableSet.of(new S3BlobStoreContextModule());

  var context = ContextBuilder.newBuilder("aws-s3")
    .credentials(config.storageKey, config.storageSecret)
    .overrides(overrides)
    .endpoint(config.endPoint.get).buildView(classOf[BlobStoreContext])
  var blobStore = context.getBlobStore
  println("Signer: " + context.getSigner.toString)

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


        if (!( blobStore.containerExists(container) )) {
          blobStore.createContainerInLocation(null, container)
        }
        val fileObj = new File(file)
        val payload = Files.asByteSource(fileObj)
        val payload_size  = payload.size()
        val payload_md5 = payload.hash(hash.Hashing.md5())
        val  contentType = tika.detect(fileObj)

        val blob = blobStore.blobBuilder(objectKey).payload(payload).contentType(contentType).contentEncoding("UTF-8").contentLength(payload_size).contentMD5(payload_md5).build()
        // blobStore.putBlob(container, blob, new PutOptions().multipart())
        blobStore.putBlob(container, blob)
        if (ttl.isDefined) {
          getSignedURL(container, objectKey, Option(ttl.get))
        } else
          blobStore.blobMetadata(container, objectKey).getUri.toString
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

  override def put(container: String, content: Array[Byte], objectKey: String, isPublic: Option[Boolean] = Option(false), isDirectory: Option[Boolean] = Option(false), ttl: Option[Int] = None, retryCount: Option[Int] = None): String = {

    try {

      if (attempt == retryCount.getOrElse(maxRetries)) {
        val message = s"Failed to upload. key: $objectKey, attempt: $attempt, maxAttempts: $retryCount. Exceeded maximum number of retries"
        throw new StorageServiceException(message)
      }

      if (!( blobStore.containerExists(container) )) {
        blobStore.createContainerInLocation(null, container)
      }
      val blob = blobStore.blobBuilder(objectKey).payload(content).contentLength(content.length).build()
      blobStore.putBlob(container, blob)
      if(isPublic.get) {
        getSignedURL(container, objectKey, Option(ttl.getOrElse(maxSignedurlTTL)))
      }
      else blobStore.getBlob(container, objectKey).getMetadata.getUri.toString
    }
    catch {
      case e: Exception => {
        Thread.sleep(attempt*2000)
        attempt += 1
        put(container, content, objectKey, isPublic, isDirectory, ttl, retryCount)
      }
    }
  }
}
