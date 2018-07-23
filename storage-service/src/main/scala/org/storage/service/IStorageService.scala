package org.storage.service

trait IStorageService {

    /*
    uploads object to a given container in cloud storage
    */
    def upload(container: String, key: String, file: String): Unit = {
        upload(container, key, file, false, 10)
    }

    /*
    uploads object with public access to a given container in cloud storage
    */
    def upload(container: String, key: String, file: String, isPublic: Boolean)

    /*
    uploads object with public access to a given container in cloud storage with a retry logic
    */
    def upload(container: String, key: String, file: String, isPublic: Boolean, retryCount: Integer)

    /*
    uploads entire directory to a given container in cloud storage
    */
    def uploadDirectory(container: String, file: String, prefix: String)

    /*
    uploads object with expiry time to a given container in cloud storage
    */
    def uploadPublicWithExpiry(container: String, key: String, file: String, expiryInDays: Int): String

    /*
    Gives a signed URL to upload object from client side
    */
    def getSignedURLToUploadObject(container: String, key: String): String

    /*
    downloads object to a given localPath from cloud storage
    */
    def download(container: String, key: String, localPath: String)

    /*
    downloads entire directory to a given localPath from cloud storage
    */
    def downloadDirectory(container: String, prefix: String, localPath: String)

    /*
    delete object from a given container in cloud storage
    */
    def deleteObject(container: String, key: String) {
        deleteObjects(container, List(key))
    }

    /*
    delete list of objects from a given container in cloud storage
    */
    def deleteObjects(container: String, keys: List[String])

    /*
    delete entire directory from a given container in cloud storage
    */
    def deleteDirectory(container: String, prefix: String)

    /*
    Gives a signed URL to access object from client side
    */
    def getPreSignedURL(container: String, key: String, expiryTimeInSecs: Long): String

    /*
    Gives a list of object keys in the given container
    */
    def getKeys(container: String, prefix: String): Array[String]

    /*
    Gives a list of object keys in the given container for given date range
    */
    def searchKeys(container: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, pattern: String = "yyyy-MM-dd"): Array[String]

    /*
    Gives a list of object with complete path in the given container for given date range
    */
    def search(container: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, pattern: String = "yyyy-MM-dd"): Array[String]

    /*
    Gives the complete object path in the given container based on cloud storage
    */
    def getPath(container: String, key: String): Array[String]

}