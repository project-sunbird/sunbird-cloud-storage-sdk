package org.sunbird.cloud.storage.service

import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.scalatest._
import org.sunbird.cloud.storage.conf.AppConf

class TestAzureStorageService extends FlatSpec {

    it should "test for azure storage" in {

        val azureService = StorageServiceFactory.getStorageService(StorageConfig("azure", AppConf.getStorageKey("azure"), AppConf.getStorageSecret("azure")))

        azureService.upload("test-container", "src/test/resources/test-data.log", "testUpload/test-blob.log")
        azureService.download("test-container", "testUpload/test-blob.log", "src/test/resources/test-azure/")

        // upload directory
        println("url of folder", azureService.upload("test-container", "src/test/resources/1234/", "testUpload/1234/", None, Option(true)))

        // downlaod directory
        azureService.download("test-container", "testUpload/1234/", "src/test/resources/test-azure/", Option(true))

        println("azure signed url", azureService.getSignedURL("test-container", "testUpload/test-blob.log", Option(600)))

        val blob = azureService.getObject("test-container", "testUpload/test-blob.log")
        println("blob details: ", blob)

        println("upload public url", azureService.upload("test-container", "src/test/resources/test-data.log", "testUpload/test-data-public.log", Option(true)))
        println("upload public with expiry url", azureService.upload("test-container", "src/test/resources/test-data.log", "testUpload/test-data-with-expiry.log", Option(true), Option(false), Option(600)))
        //        println("signed path to upload from external client", azureService.getSignedURLToUploadObject("test-container", "src/test/resources/test-data.log", "testUpload/test-data-public1.log"))

        val keys = azureService.searchObjectkeys("test-container", "testUpload/1234/")
        keys.foreach(f => println(f))
        val blobs = azureService.searchObjects("test-container", "testUpload/1234/")
        blobs.foreach(f => println(f))

        // delete directory
        azureService.deleteObject("test-container", "testUpload/1234/", Option(true))
        azureService.deleteObject("test-container", "testUpload/test-blob.log")
        //azureUtil.deleteObject("test-container", "testUpload/test-data-public.log")
        //azureUtil.deleteObject("test-container", "testUpload/test-data-with-expiry.log")

        azureService.upload("test-container", "src/test/resources/test-extract.zip", "testUpload/test-extract.zip")
        azureService.copyObjects("test-container", "testUpload/test-extract.zip", "test-container", "testDuplicate/test-extract.zip")

        azureService.extractArchive("test-container", "testUpload/test-extract.zip", "testUpload/test-extract/")

        azureService.closeContext()
    }
}
