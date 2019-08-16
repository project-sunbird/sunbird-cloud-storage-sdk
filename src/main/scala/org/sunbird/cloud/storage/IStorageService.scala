package org.sunbird.cloud.storage

import org.sunbird.cloud.storage.Model.Blob

import scala.concurrent.{ExecutionContext, Future}

trait IStorageService {

    def uploadFolder(container: String, file: String, objectKey: String, isPublic: Option[Boolean] = Option(false), ttl: Option[Int] = None, retryCount: Option[Int] = None, attempt: Int = 1)(implicit execution: ExecutionContext) : Future[List[String]];

    /**
      *
      * Upload a file/folder to cloud
      *
      * @param container String - The container/bucket to upload the file to.
      * @param file String - The file path.
      * @param objectKey String - The destination key/path to upload the file to. If the path exists it will be overwritten.
      * @param isDirectory Option[Boolean] - Whether the file is a directory and need to upload folder recursively?
      * @param attempt
      * @param retryCount Option[Int] - Number of times the upload will be retried before failing. Defaults to global configuration "max.retries"
      * @param ttl Option[Int] - The ttl/expiry for the file. Optional and default is never expires
      * @return String - The url of the file/folder uploaded
      */
    def upload(container: String, file: String, objectKey: String, isDirectory: Option[Boolean] = Option(false),  attempt: Option[Int] = Option(1), retryCount: Option[Int] = None, ttl: Option[Int] = None): String

        
    /**
     * Put a blob in the cloud with the given content data. The difference between this and <code>upload()</code> method is that this method takes in byte array and sets is payload for the object.
     * 
     * @param container String - The container/bucket to upload the file to.
     * @param content String - The byte array of an object.
     * @param objectKey String - The destination key/path to upload the file to. If the path exists it will be overwritten.
     * @param isPublic Option[Boolean] - Whether the file should have public read access? Optional and defaults to false.
     * @param isDirectory Option[Boolean] - Whether the file is a directory and need to upload folder recursively? Optional and defaults to false.
     * @param ttl Option[Int] - The ttl/expiry for the file. Optional and default is never expires
     * @param retryCount Option[Int] - Number of times the upload will be retried before failing. Defaults to "max.retries" defined in global configuration 
     * 
     * @return String - The url of the file/folder uploaded
     */            
    def put(container: String, content: Array[Byte], objectKey: String, isPublic: Option[Boolean] = Option(false), isDirectory: Option[Boolean] = Option(false), 
            ttl: Option[Int] = None, retryCount: Option[Int] = None): String

    /**
     * Get pre-signed URL to access an object in the cloud store.
     * 
     * @param container String - The container/bucket of the file
     * @param objectKey String - The key/path of the file to pre-sign
     * @param ttl Option[Int] - The ttl/expiry for the pre-signed URL. Defaults to "max.signedurl.ttl" defined in global configuration*
     * @param permission String - The permission of pre-signed url the values are w (write), r (read). Defaults to "read".
     *
     * @return String - The pre-signed url
     */
    def getSignedURL(container: String, objectKey: String, ttl: Option[Int] = None, permission: Option[String] = Option("r")): String

    /**
     * Download file/folder from cloud storage
     * 
     * @param container String - The container/bucket of the file
     * @param objectKey String - The key/path of the file to download from
     * @param localPath String - The local destination path to download to
     * @param isDirectory Option[Boolean] - Whether the file is a directory and need to be downloaded recursively? Optional and defaults to false.
     */
    def download(container: String, objectKey: String, localPath: String, isDirectory: Option[Boolean] = Option(false))

    /**
     * Delete an object from the cloud store
     * 
     * @param container String - The container/bucket of the object.
     * @param objectKey String - The object key to delete.
     * @param isDirectory Option[Boolean] - Whether the object is a directory and need to be deleted recursively? Optional and defaults to false.
     */
    def deleteObject(container: String, objectKey: String, isDirectory: Option[Boolean] = Option(false))

    /**
     * Delete objects from the cloud store 
     * 
     * @param container String - The container/bucket of the objects
     * @param objectKeys List[(String, Boolean)] - The objects to delete. The tuple contains the object key to be deleted and whether the object is a folder so that recursive delete is triggered.
     */
    def deleteObjects(container: String, objectKeys: List[(String, Boolean)])

    /**
     * Copy objects from one container to another container or between different folders within the same container. This assumes that the credentials provided have access to both containers.
     * 
     * @param fromContainer String - The container to copy the object from.
     * @param fromKey String - The object prefix to copy from.
     * @param toContainer String - The container to copy the object to.
     * @param toKey String - The object prefix to copy to.
     * @param isDirectory Option[Boolean] - Whether the copy is a file or folder? Defaults to false i.e copy one file.
     */
    def copyObjects(fromContainer: String, fromKey: String, toContainer: String, toKey: String, isDirectory: Option[Boolean] = Option(false))


    /**
     * Remote extract a archived file on cloud storage to a given folder within the same container. 
     * 
     * @param container String - The container/bucket of the archive file.
     * @param objectKey String - The blob object archive file
     * @param toKey String - The destination folder on the bucket/container to extract to.
     */
    def extractArchive(container: String, objectKey: String, toKey: String)

    /**
     * Get the blob object details
     * 
     * @param container String - The container/bucket of the file
     * @param objectKey String - The key/path of the blob object
     * @param withPayload Option[Boolean] - Get payload as well while fetching details. Defaults to false.
     * 
     * @return Blob - The blob object.
     */
    def getObject(container: String, objectKey: String, withPayload: Option[Boolean] = Option(false)): Blob

    /**
     * List objects from cloud storage for a given prefix.
     * 
     * @param container String - The container/bucket
     * @param prefix String - The object prefix to list objects. The prefix can be folder or pattern.
     * @param withPayload Option[Boolean] - Does the listing of objects include payload as well? Defaults to false
     * 
     * @return List[Blob] - The blob objects for the given prefix.
     */
    def listObjects(container: String, prefix: String, withPayload: Option[Boolean] = Option(false)): List[Blob]

    /**
     * List object keys from cloud storage for a given prefix. Similar to <code>listObjects()</code>
     * 
     * @param container String - The container/bucket
     * @param _prefix String - The object prefix to list objects. The prefix can be folder or pattern.
     * 
     * @return List[Blob] - The blob objects for the given prefix.
     */
    def listObjectKeys(container: String, _prefix: String): List[String]

    /**
     * Search for objects for a given prefix and return only keys. Specifically used for telemetry files as the files are prefixed by sync date. 
     * By design the payload is not included in the search. If payload is required use <code>getObject()</code> method after the search is complete.
     * 
     * // TODO: Provide examples of all combinations of fromDate, toDate and delta
     * 
     * @param container String - The container/bucket.
     * @param prefix String - The prefix to search on.
     * @param fromDate Option[String] - The date to search from. Optional
     * @param toDate Option[String] - The date to search to. Optional
     * @param delta Option[Int] - The delta to search from given a from date or to date. Optional. If delta is provided and both fromDate and toDate are empty, the toDate will be defaulted to current date
     * @param pattern String - The date pattern of from and to date. Defaulst to "yyyy-MM-dd"
     * 
     * @return List[Blob] - The object keys
     */
    def searchObjects(container: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, 
            pattern: String = "yyyy-MM-dd"): List[Blob]

    /**
     * Similar to <code>searchObjects()</code>. The only difference is that this method only returns the object keys instead of full blob objects.
     * 
     * @param container String - The container/bucket.
     * @param prefix String - The prefix to search on.
     * @param fromDate Option[String] - The date to search from. Optional
     * @param toDate Option[String] - The date to search to. Optional
     * @param delta Option[Int] - The delta to search from given a from date or to date. Optional. If delta is provided and both fromDate and toDate are empty, the toDate will be defaulted to current date
     * @param pattern String - The date pattern of from and to date. Defaulst to "yyyy-MM-dd"
     * 
     * @return List[String] - The object keys
     */
    def searchObjectkeys(container: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, 
            pattern: String = "yyyy-MM-dd"): List[String]

    /**
     * Get HDFS compatible file paths to be used in tech stack like Spark. 
     * For ex: for S3 the file path is prefixed with s3n://<bucket>/<key> and for Azure blob storage it would be wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<key>/
     * 
     * @param container String - The container/bucket of the objects
     * @param objects List[Blob] - The Blob objects in the given container
     * 
     * @return List[String] - HDFS compatible file paths. 
     */
    def getPaths(container: String, objects: List[Blob]): List[String]

    /**
      * Get the blob object data.
      *
      * @param container String - The container/bucket of the file
      * @param objectKey String - The key/path of the blob object
      *
      * @return Array[String] - object data
      */
    def getObjectData(container: String, objectKey: String): Array[String]


    /**
      * Get the URI of the given prefix
      *
      * @param container
      * @param _prefix
      * @param isDirectory
      * @return String - URI of the given prefix
      */
    def getUri(container: String, _prefix: String, isDirectory: Option[Boolean] = Option(false)): String

}