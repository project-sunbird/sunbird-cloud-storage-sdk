package org.sunbird.cloud.storage.service

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

class TestGcloudStorageService extends FlatSpec with Matchers {

  it should "test for gcloud storage" in {

    val gsService = StorageServiceFactory.getStorageService(StorageConfig("gcloud", AppConf.getStorageKey("gcloud"), AppConf.getStorageSecret("gcloud")))

    val storageContainer = AppConf.getConfig("gcloud_storage_container")

//    gsService.upload(storageContainer, "src/test/resources/test-data.log", "testUpload/test-blob.log", Option(false), Option(1), Option(2), None)
//    gsService.upload(storageContainer, "src/test/resources/test-extract.zip", "testUpload/test-extract.zip", Option(false), Option(1), Option(2), None)
//
//    // upload directory
//    println("url of folder", gsService.upload(storageContainer, "src/test/resources/1234/", "testUpload/1234/", Option(true), Option(1), Option(2), None))
//
//    // downlaod directory
//    gsService.download(storageContainer, "testUpload/1234/", "src/test/resources/test-gcloud/", Option(true))
//
//    println("gcloud signed url", gsService.getSignedURL(storageContainer, "testUpload/test-blob.log", Option(600)))
//
//    val blob = gsService.getObject(storageContainer, "testUpload/test-blob.log")
//    println("blob details: ", blob)
//
//    val folderUri = gsService.getUri(storageContainer, "testUpload", Option(true))
//    println("Folder URI: ", folderUri)
//    val fileUri = gsService.getUri(storageContainer, "testUpload/test-blob.log", Option(false))
//    println("File URI: ", fileUri)
//
//    val keys = gsService.searchObjectkeys(storageContainer, "testUpload/1234/")
//    keys.foreach(f => println(f))
//    val blobs = gsService.searchObjects(storageContainer, "testUpload/1234/")
//    blobs.foreach(f => println(f))
//
//    val objData = gsService.getObjectData(storageContainer, "testUpload/test-blob.log")
//    objData.length should be(18)

//    println("signed path to upload from external client", gsService.getPutSignedURL(storageContainer, "testUpload/test-data-public1.log", Option(600), Option("w"), Option("text/plain")))

//    gsService.copyObjects(storageContainer, "testUpload/1234/", storageContainer, "testDuplicate/1234/", Option(true))
//    gsService.extractArchive(storageContainer, "testUpload/test-extract.zip", "testUpload/test-extract/")

    // delete directory
//    gsService.deleteObject(storageContainer, "testUpload/1234/", Option(true))
//    gsService.deleteObject(storageContainer, "testUpload/test-blob.log")
//    gsService.deleteObject(storageContainer, "testUpload/test-extract.zip")
//    gsService.deleteObject(storageContainer, "testDuplicate/", Option(true))
//    gsService.deleteObject(storageContainer, "testUpload/test-extract/", Option(true))

    val caught1 =
      intercept[StorageServiceException]{
        gsService.getSignedURL(storageContainer, "testUpload/test-data-public1.log", Option(600), Option("w"))
      }
    assert(caught1.getMessage.contains("getSignedURL method is not supported for GCP. Please use getPutSignedURL with contentType."))

    val caught2 =
      intercept[StorageServiceException]{
        gsService.upload(storageContainer, "src/test/resources/1234/test-blob.log", "testUpload/1234/", Option(false), Option(5),Option(2), None)
      }
    assert(caught2.getMessage.contains("Failed to upload."))

    gsService.closeContext()
  }
}
