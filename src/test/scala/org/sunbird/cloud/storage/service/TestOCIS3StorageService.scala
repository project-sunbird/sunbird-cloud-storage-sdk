package org.sunbird.cloud.storage.service

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

class TestOCIS3StorageService  extends FlatSpec with Matchers {

  it should "test for OCIS3 storage" in {

    val storageConfig = StorageConfig("oci", AppConf.getStorageKey("oci"), AppConf.getStorageSecret("oci"), AppConf.getEndPoint("oci"), AppConf.getRegion("oci"))
    val ociS3Service = StorageServiceFactory.getStorageService(storageConfig)

    val storageContainer = AppConf.getConfig("oci_storage_container")

    // Use this exception block to execute the test cases successfully when it has invalid configuration.
    val caught =
        intercept[StorageServiceException]{
          ociS3Service.upload(storageContainer, "src/test/resources/1234/test-blob.log", "testUpload/1234/", Option(false),Option(5), Option(2), None)
        }
    assert(caught.getMessage.contains("Failed to upload."))

    /**
     * Use the below complete block when we have the valid configuration and
     * to test the OCI functionality.
     */
    /**
    ociS3Service.upload(storageContainer, "src/test/resources/test-data.log", "testUpload/test-blob.log")
    ociS3Service.download(storageContainer, "testUpload/test-blob.log", "src/test/resources/test-s3/")

    // upload directory
    println("url of folder", ociS3Service.upload(storageContainer, "src/test/resources/1234/", "testUpload/1234/", Option(true)))

    // downlaod directory
    ociS3Service.download(storageContainer, "testUpload/1234/", "src/test/resources/test-s3/", Option(true))

    println("OCI S3 signed url", ociS3Service.getSignedURL(storageContainer, "testUpload/test-blob.log", Option(600)))

    val blob = ociS3Service.getObject(storageContainer, "testUpload/test-blob.log")
    println("blob details: ", blob)

    println("upload public url", ociS3Service.upload(storageContainer, "src/test/resources/test-data.log", "testUpload/test-data-public.log", Option(true)))
    println("upload public with expiry url", ociS3Service.upload(storageContainer, "src/test/resources/test-data.log", "testUpload/test-data-with-expiry.log", Option(false)))
    println("signed path to upload from external client", ociS3Service.getSignedURL(storageContainer, "testUpload/test-data-public1.log", Option(600), Option("w")))

    val keys = ociS3Service.searchObjectkeys(storageContainer, "testUpload/1234/")
    keys.foreach(f => println(f))
    val blobs = ociS3Service.searchObjects(storageContainer, "testUpload/1234/")
    blobs.foreach(f => println(f))

    val objData = ociS3Service.getObjectData(storageContainer, "testUpload/test-blob.log")
    objData.length should be(18)

    // delete directory
    ociS3Service.deleteObject(storageContainer, "testUpload/1234/", Option(true))
    ociS3Service.deleteObject(storageContainer, "testUpload/test-blob.log")

    ociS3Service.upload(storageContainer, "src/test/resources/test-extract.zip", "testUpload/test-extract.zip")
    ociS3Service.copyObjects(storageContainer, "testUpload/test-extract.zip", storageContainer, "testDuplicate/test-extract.zip")

    ociS3Service.extractArchive(storageContainer, "testUpload/test-extract.zip", "testUpload/test-extract/")
    */
    ociS3Service.closeContext()
  }

}
