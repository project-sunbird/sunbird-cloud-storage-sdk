package org.sunbird.cloud.storage.service

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, BlobInfo, HttpMethod, Storage, StorageOptions}
import com.google.common.io.Files
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.factory.StorageConfig
import org.apache.tika.metadata.HttpHeaders
import org.apache.tika.mime.MimeTypes

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class GcloudStorageService(config: StorageConfig) extends BaseStorageService  {

  var context = ContextBuilder.newBuilder("google-cloud-storage").credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
  var blobStore = context.getBlobStore

  override def getPaths(container: String, objects: List[Blob]): List[String] = {
    objects.map{f => "gs://" + container + "/" + f.key}
  }

  // Overriding upload methos since multipart upload not working for GCP
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
      val host = "https://storage.googleapis.com/"
      val name = blob.metadata.get("name").get.asInstanceOf[String]
      host + container + "/" + name
    } else
      throw new StorageServiceException("uri not available for the given prefix: "+ _prefix)
  }
    /**
   * Method to get V4 Signed URL when storage is GCP
   * @param re
   * @return
   */
  override def getPutSignedURL(container: String, objectKey: String,  ttl: Option[Int], permission: Option[String] = Option("r"),
                               contentType: Option[String] = Option("application/octet-stream"), additionalParams: Option[Map[String,String]] = None): String = {
    if(additionalParams.isEmpty) {
      throw new StorageServiceException("Missing google credentials params.")
    }
    // expiry time validation as TTL cannot be greater than 604800
    // expiry time will be set to default value of 604800 if greater than 604800
    val properties = additionalParams.get
    // prepare common credentials, storage and blob info once
    val credentials = ServiceAccountCredentials.fromPkcs8(properties.get("clientId").get, properties.get("clientEmail").get, properties.get("privateKeyPkcs8").get,
      properties.get("privateKeyIds").get, new java.util.ArrayList[String]())
    val storage = StorageOptions.newBuilder.setProjectId(properties.get("projectId").get).setCredentials(credentials).build.getService
    val blobInfo = BlobInfo.newBuilder(BlobId.of(container, objectKey)).build
    val expiryTime = if(ttl.get > maxSignedurlTTL) maxSignedurlTTL else ttl.get
    //creating signed url
    val effectivePermission = permission.getOrElse("r").toLowerCase
    val url = effectivePermission match {
      case "w" =>
        // extract optional flag to indicate resumable/chunked upload, defaulting to false
        val isChunkedUpload: Boolean = properties.get("chunked").map(_.toLowerCase).contains("true")
        if (isChunkedUpload) {
          // Only for chunked uploads, use resumable POST and include required headers
          val extensionHeaders = Map(HttpHeaders.CONTENT_TYPE -> contentType.getOrElse(MimeTypes.OCTET_STREAM), "x-goog-resumable" -> "start")
          storage.signUrl(
            blobInfo,
            expiryTime,
            TimeUnit.SECONDS,
            Storage.SignUrlOption.httpMethod(HttpMethod.POST),
            Storage.SignUrlOption.withV4Signature,
            Storage.SignUrlOption.withExtHeaders(extensionHeaders.asJava)
          )
        } else {
          // Original behavior: simple PUT without extra headers
          storage.signUrl(
            blobInfo,
            expiryTime,
            TimeUnit.SECONDS,
            Storage.SignUrlOption.httpMethod(HttpMethod.PUT),
            Storage.SignUrlOption.withV4Signature
          )
        }
      case _ =>
        // Default and for any invalid value: generate READ signed URL (GET)
        storage.signUrl(
          blobInfo,
          expiryTime,
          TimeUnit.SECONDS,
          Storage.SignUrlOption.httpMethod(HttpMethod.GET),
          Storage.SignUrlOption.withV4Signature
        )
    }
    url.toString;
  }

  // Override listObjectKeys to avoid JClouds parsing issues
  override def listObjectKeys(container: String, prefix: String): List[String] = {
    try {
      println(s"Listing objects with prefix '$prefix' in container '$container' using direct HTTP")

      // Use the Google Cloud Storage JSON API
      val host = "https://storage.googleapis.com/storage/v1/b"
      val url = new java.net.URL(s"${host}/${container}/o?prefix=${java.net.URLEncoder.encode(prefix, "UTF-8")}")

      println(s"Querying API URL: ${url}")

      val connection = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setRequestProperty("Accept", "application/json")

      val responseCode = connection.getResponseCode
      if (responseCode != 200) {
        println(s"Failed to list objects. Response code: $responseCode")
        return List.empty[String]
      }

      // Parse the JSON response
      val responseStream = connection.getInputStream
      val responseString = scala.io.Source.fromInputStream(responseStream).mkString
      responseStream.close()

      // Parse items from the response
      val items = parseJsonItems(responseString)

      if (items.isEmpty) {
        println("No items found in the response")
        List.empty[String]
      } else {
        println(s"Found ${items.size} objects")
        items
      }
    } catch {
      case e: Exception =>
        println(s"Error listing objects: ${e.getMessage}")
        e.printStackTrace()
        List.empty[String]
    }
  }

  // Helper method to parse JSON without external dependencies
  private def parseJsonItems(json: String): List[String] = {
    try {
      // Simple regex-based JSON parsing to extract names
      val namePattern = """"name"\s*:\s*"([^"]+)"""".r
      val matches = namePattern.findAllMatchIn(json)
      matches.map(_.group(1)).toList
    } catch {
      case e: Exception =>
        println(s"Error parsing JSON: ${e.getMessage}")
        e.printStackTrace()
        List.empty[String]
    }
  }

  override def download(container: String, objectKey: String, localPath: String, isDirectory: Option[Boolean] = Option(false)): Unit = {
    try {
      if(isDirectory.get) {
        println(s"Starting GCP directory download operation for: container=$container, prefix=$objectKey to $localPath")
        val objects = listObjectKeys(container, objectKey)

        if (objects.isEmpty) {
          println(s"No objects found with prefix $objectKey in container $container")
          return
        }

        println(s"Found ${objects.size} objects to download from prefix $objectKey")

        // Ensure local directory exists
        val downloadDir = new File(localPath)
        if (!downloadDir.exists()) {
          downloadDir.mkdirs()
        }

        var successCount = 0
        var failureCount = 0

        // Download each object
        for (obj <- objects) {
          try {
            // Calculate the relative path from the prefix
            val relativePath = if (obj.startsWith(objectKey)) {
              obj.substring(objectKey.length)
            } else {
              obj
            }

            // Create full local path with directories
            val fullLocalPath = new File(localPath, relativePath)
            val parentDir = fullLocalPath.getParentFile
            if (parentDir != null && !parentDir.exists()) {
              parentDir.mkdirs()
            }

            println(s"Downloading object: $obj to ${fullLocalPath.getAbsolutePath}")
            val result = downloadFile(container, obj, fullLocalPath.getAbsolutePath)

            if (result) {
              successCount += 1
            } else {
              failureCount += 1
            }
          } catch {
            case e: Exception =>
              println(s"Error downloading object $obj: ${e.getMessage}")
              failureCount += 1
          }
        }

        println(s"Directory download completed. Successfully downloaded $successCount files. Failed to download $failureCount files.")
      } else {
        println(s"Starting GCP file download operation for: container=$container, objectKey=$objectKey to $localPath")

        val fileName = objectKey.split("/").last
        val downloadFilePath = new File(localPath, fileName).getAbsolutePath

        downloadFile(container, objectKey, downloadFilePath)
      }
    } catch {
      case e: Exception =>
        println(s"Error during download: ${e.getMessage}")
        e.printStackTrace()
        throw new StorageServiceException(s"Failed to download object $objectKey from container $container: ${e.getMessage}", e)
    }
  }

  /**
   * Downloads a single file from GCP storage
   *
   * @param container The storage container/bucket name
   * @param objectKey The object key to download
   * @param destinationPath The full path where the file should be saved
   * @return true if download was successful, false otherwise
   */
  private def downloadFile(container: String, objectKey: String, destinationPath: String): Boolean = {
    try {
      // Ensure parent directory exists
      val downloadFile = new File(destinationPath)
      val parentDir = downloadFile.getParentFile
      if (parentDir != null && !parentDir.exists()) {
        parentDir.mkdirs()
      }

      // Handle the download using a direct HTTP approach to avoid JClouds parsing issues
      val host = "https://storage.googleapis.com/"
      val url = new java.net.URL(s"${host}${container}/${objectKey}")

      println(s"Downloading from URL: ${url}")

      val connection = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")

      val responseCode = connection.getResponseCode
      if (responseCode != 200) {
        println(s"Failed to download $objectKey. Response code: $responseCode")
        return false
      }

      val inputStream = connection.getInputStream
      val outputStream = new FileOutputStream(downloadFile)

      try {
        val buffer = new Array[Byte](8192)
        var bytesRead = inputStream.read(buffer)
        while (bytesRead != -1) {
          outputStream.write(buffer, 0, bytesRead)
          bytesRead = inputStream.read(buffer)
        }
        println(s"Successfully downloaded file to ${downloadFile.getAbsolutePath}")
        true
      } finally {
        if (inputStream != null) inputStream.close()
        if (outputStream != null) outputStream.close()
      }
    } catch {
      case e: Exception =>
        println(s"Error downloading object $objectKey: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

}