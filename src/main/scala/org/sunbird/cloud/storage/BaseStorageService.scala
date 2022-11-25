package org.sunbird.cloud.storage

import org.jclouds.blobstore._
import com.google.common.io._
import java.io._

import org.apache.commons.io.FilenameUtils
import org.apache.tika.Tika
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.util.{CommonUtil, JSONUtils}

import collection.JavaConverters._
import org.jclouds.blobstore.options.ListContainerOptions.Builder.{afterMarker, prefix, recursive}
import org.sunbird.cloud.storage.Model.Blob
import org.jclouds.blobstore.options.{CopyOptions, PutOptions}
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

trait BaseStorageService extends IStorageService {

    var context: BlobStoreContext
    var blobStore: BlobStore
    var maxRetries: Int = 2
    var maxSignedurlTTL: Int = 604800
    var attempt = 0
    var maxContentLength = 0
    val tika = new Tika()

    def filesList(file: File): List[File] = {
        if (file.exists && file.isDirectory) {
            val files = file.listFiles.filter(_.isDirectory).flatMap(f => filesList(f)).toList
            files ++ file.listFiles.filter(_.isFile).toList;
        } else if (file.exists && file.isFile) {
          List[File](file)
        } else {
            List[File]();
        }
    }

    override def uploadFolder(container: String, file: String, objectKey: String, isPublic: Option[Boolean] = Option(false), ttl: Option[Int] = None, retryCount: Option[Int] = None, attempt: Int = 1) (implicit execution: ExecutionContext) : Future[List[String]] = {
        val d = new File(file)
        val files = filesList(d)
        val futures = files.map {f =>
            Future {
                val key = objectKey + f.getAbsolutePath.split(d.getAbsolutePath + File.separator).last
                upload(container, f.getAbsolutePath, key, Option(false), Option(attempt), retryCount, ttl)
            }
        };
        Future.sequence(futures);
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

                blobStore.createContainerInLocation(null, container)
                val fileObj = new File(file)
                val payload = Files.asByteSource(fileObj)
                val  contentType = tika.detect(fileObj)
                val blob = blobStore.blobBuilder(objectKey).payload(payload).contentType(contentType).contentEncoding("UTF-8").contentLength(payload.size()).build()
                blobStore.putBlob(container, blob, new PutOptions().multipart())
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

            blobStore.createContainerInLocation(null, container)
            val blob = blobStore.blobBuilder(objectKey).payload(content).contentLength(content.length).build()
            blobStore.putBlob(container, blob, new PutOptions().multipart())
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

    override def getSignedURL(container: String, objectKey: String, ttl: Option[Int] = None, permission: Option[String] = Option("r")): String = {
        if (context.getBlobStore.toString.contains("google")) {
            throw new StorageServiceException("getSignedURL method is not supported for GCP. Please use getPutSignedURL with contentType.", new Exception())
        }
        else {
            if (permission.getOrElse("").equalsIgnoreCase("w")) {
                context.getSigner.signPutBlob(container, blobStore.blobBuilder(objectKey).forSigning().contentLength(maxContentLength).build(), ttl.getOrElse(maxSignedurlTTL).asInstanceOf[Number].longValue()).getEndpoint.toString
            } else {
                context.getSigner.signGetBlob(container, objectKey, ttl.getOrElse(maxSignedurlTTL)).getEndpoint.toString
            }
        }
    }

    override def getSignedURLV2(container: String, objectKey: String, ttl: Option[Int] = None, permission: Option[String] = Option("r"), contentType: Option[String] = Option("text/plain")): String = {
        if (context.getBlobStore.toString.contains("google")) {
            getPutSignedURL(container, objectKey, Option(maxSignedurlTTL), None, contentType)
        } else {
            getSignedURL(container, objectKey, ttl, permission)
        }
    }

    def getPutSignedURL(container: String, objectKey: String, ttl: Option[Int] = None, permission: Option[String] = Option("r"), contentType: Option[String] = Option("text/plain")): String = {
        if (permission.getOrElse("").equalsIgnoreCase("w")) {
            context.getSigner.signPutBlob(container, blobStore.blobBuilder(objectKey).forSigning().contentLength(maxContentLength).contentType(contentType.get).build(), ttl.getOrElse(maxSignedurlTTL).asInstanceOf[Number].longValue()).getEndpoint.toString
        } else {
            context.getSigner.signGetBlob(container, objectKey, ttl.getOrElse(maxSignedurlTTL)).getEndpoint.toString
        }
    }

    override def download(container: String, objectKey: String, localPath: String, isDirectory: Option[Boolean] = Option(false)) = {
        try {
            if(isDirectory.get) {
                val objects = listObjectKeys(container, objectKey)
                for (obj <- objects) {
                    val file = FilenameUtils.getName(obj);
                    val fileObj = blobStore.getBlob(container, obj)
                    val downloadPath = localPath + FilenameUtils.getPath(obj).split("/").last + "/";
                    CommonUtil.copyFile(fileObj.getPayload.getInput, downloadPath.replaceAll("//", "/"), file);
                }
            }
            else {
                val inStream = blobStore.getBlob(container, objectKey).getPayload.getInput
                val fileName = objectKey.split("/").last
                CommonUtil.copyFile(inStream, localPath, fileName);
            }
        } catch {
            case e: Exception =>
                throw new StorageServiceException(e.getMessage, e)
        }
    }

    override def deleteObject(container: String, objectKey: String, isDirectory: Option[Boolean] = Option(false)) = {
        try {
            deleteObjects(container, List((objectKey, isDirectory.get)))
        } catch {
            case e: Exception =>
                throw new StorageServiceException(e.getMessage, e)
        }
    }

    override def deleteObjects(container: String, objectKeys: List[(String, Boolean)]) = {
        try {
            for (obj <- objectKeys) {
                if(obj._2) {
                    val objList = blobStore.list(container, prefix(obj._1).recursive()).asScala.map(f => f.getName).toList
                    blobStore.removeBlobs(container, objList.asJavaCollection)
                }
                else {
                    blobStore.removeBlobs(container, List(obj._1).asJavaCollection)
                }
            }
        } catch {
            case e: Exception =>
                throw new StorageServiceException(e.getMessage, e)
        }
    }

    override def getObject(container: String, objectKey: String, withPayload: Option[Boolean] = Option(false)): Blob = {
        try {
            val blob = blobStore.getBlob(container, objectKey)
            val objData = blob.getMetadata
            val metaData = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(objData))
            val payload = if(withPayload.get) Option(blob.getPayload.getContentMetadata.getContentMD5AsHashCode.asBytes()) else None
            Blob(objectKey, objData.getContentMetadata.getContentLength, objData.getLastModified, metaData, payload)
        } catch {
            case e: Exception =>
                throw new StorageServiceException(e.getMessage, e)
        }
    }

    override def listObjects(container: String, prefix: String, withPayload: Option[Boolean] = Option(false)): List[Blob] = {
        try {
            val objects = listObjectKeys(container, prefix)
            objects.map { obj =>
                getObject(container, obj, withPayload)
            }
        }
        catch {
            case e: Exception =>
                throw new StorageServiceException(e.getMessage, e)
        }
    }

    override def listObjectKeys(container: String, _prefix: String): List[String] = {
        val fileNames = ListBuffer[String]()
        val containerOpts = recursive().prefix(_prefix)
        var marker: Option[String] = None
        do {
            if (marker.exists(_.trim.nonEmpty)) containerOpts.afterMarker(marker.getOrElse(""))
            val pageSet = blobStore.list(container, containerOpts)
            fileNames ++= pageSet.asScala.map(f => f.getName).toList
            marker = Option(pageSet.getNextMarker)
        } while (marker.isDefined)
        fileNames.toList
    }

    override def searchObjects(container: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, pattern: String = "yyyy-MM-dd"): List[Blob] = {
        val from = if (delta.nonEmpty) CommonUtil.getStartDate(toDate, delta.get) else fromDate;
        if (from.nonEmpty) {
            val dates = CommonUtil.getDatesBetween(from.get, toDate, pattern)
            val paths = for (date <- dates) yield {
                listObjects(container, prefix + date)
            }
            paths.flatMap { x => x.map { x => x } }.toList
        } else {
            listObjects(container, prefix)
        }
    }

    override def searchObjectkeys(container: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, pattern: String = "yyyy-MM-dd"): List[String] = {
//        val objectList = searchObjects(container, prefix, fromDate, toDate, delta, pattern)
//        getPaths(container, objectList);
        val from = if (delta.nonEmpty) CommonUtil.getStartDate(toDate, delta.get) else fromDate;
        if (from.nonEmpty) {
            val dates = CommonUtil.getDatesBetween(from.get, toDate, pattern);
            val paths = for (date <- dates) yield {
                listObjectKeys(container, prefix + date)
            }
            paths.flatMap { x => x.map { x => x } }.toList
        } else {
            listObjectKeys(container, prefix)
        }
    }

    override def copyObjects(fromContainer: String, fromKey: String, toContainer: String, toKey: String, isDirectory: Option[Boolean] = Option(false)): Unit = {
        if(isDirectory.get) {
            val updatedFromKey = if(fromKey.endsWith("/")) fromKey else fromKey+"/"
            val updatedToKey = if(toKey.endsWith("/")) toKey else toKey+"/"
            val objectKeys = listObjectKeys(fromContainer, updatedFromKey)
            for (obj <- objectKeys) {
                val objName = obj.replace(updatedFromKey, "")
                blobStore.copyBlob(fromContainer, obj, toContainer, updatedToKey+objName, CopyOptions.NONE)
            }
        }
        else blobStore.copyBlob(fromContainer, fromKey, toContainer, toKey, CopyOptions.NONE)
    }

    override def extractArchive(container: String, objectKey: String, toKey: String): Unit = {
        try {
            val localPath = AppConf.getConfig("local_extract_path")
            download(container, objectKey, localPath, Option(false))
            val localFolder = localPath + "/" + toKey.split("/").last
            CommonUtil.unZip(localPath + "/" + objectKey.split("/").last, localFolder)
            upload(container, localFolder, toKey, Option(true))
        }
        catch {
            case e: Exception =>
                throw new StorageServiceException(e.getMessage, e)
        }
    }

    override def getObjectData(container: String, objectKey: String): Array[String] = {

        try {
            val inStream = blobStore.getBlob(container, objectKey).getPayload.openStream()
            scala.io.Source.fromInputStream(inStream).getLines().toArray
        } catch {
            case e: Exception =>
                throw new StorageServiceException(e.getMessage, e)
        }
    }

    override def getUri(container: String, _prefix: String, isDirectory: Option[Boolean] = Option(false)): String = {
        val keys = listObjectKeys(container, _prefix);
        if (keys.isEmpty)
            throw new StorageServiceException("The given _prefix is incorrect: " + _prefix)
        val prefix = keys.head
        val blob = getObject(container, prefix, Option(false))
        val uri = blob.metadata.get("uri")
        if (!uri.isEmpty) {
            uri.get.asInstanceOf[String].split(_prefix).head + _prefix
        } else
            throw new StorageServiceException("uri not available for the given prefix: "+ _prefix)
    }

    def closeContext() = {
        context.close()
    }

}
