package org.sunbird.cloud.storage.service

import org.scalatest.FlatSpec
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

class TestS3StorageService extends FlatSpec {

    it should "test for s3 storage" in {

        val s3Service = StorageServiceFactory.getStorageService(StorageConfig("s3", AppConf.getStorageKey("s3"), AppConf.getStorageSecret("s3")))

        s3Service.upload("ekstep-dev-data-store", "src/test/resources/test-data.log", "testUpload/test-blob.log")
        s3Service.download("ekstep-dev-data-store", "testUpload/test-blob.log", "src/test/resources/test-s3/")

        // upload directory
        println("url of folder", s3Service.upload("ekstep-dev-data-store", "src/test/resources/1234/", "testUpload/1234/", None, Option(true)))

        // downlaod directory
        s3Service.download("ekstep-dev-data-store", "testUpload/1234/", "src/test/resources/test-s3/", Option(true))

        println("azure signed url", s3Service.getSignedURL("ekstep-dev-data-store", "testUpload/test-blob.log", Option(600)))

        val blob = s3Service.getObject("ekstep-dev-data-store", "testUpload/test-blob.log")
        println("blob details: ", blob)

        println("upload public url", s3Service.upload("ekstep-dev-data-store", "src/test/resources/test-data.log", "testUpload/test-data-public.log", Option(true)))
        println("upload public with expiry url", s3Service.upload("ekstep-dev-data-store", "src/test/resources/test-data.log", "testUpload/test-data-with-expiry.log", Option(true), Option(false), Option(600)))
        //        println("signed path to upload from external client", s3Service.getSignedURLToUploadObject("test-container", "src/test/resources/test-data.log", "testUpload/test-data-public1.log"))

        val keys = s3Service.searchObjectkeys("ekstep-dev-data-store", "testUpload/1234/")
        keys.foreach(f => println(f))
        val blobs = s3Service.searchObjects("ekstep-dev-data-store", "testUpload/1234/")
        blobs.foreach(f => println(f))

        // delete directory
        s3Service.deleteObject("ekstep-dev-data-store", "testUpload/1234/", Option(true))
        s3Service.deleteObject("ekstep-dev-data-store", "testUpload/test-blob.log")
        //s3Service.deleteObject("ekstep-dev-data-store", "testUpload/test-data-public.log")
        //s3Service.deleteObject("ekstep-dev-data-store", "testUpload/test-data-with-expiry.log")

        s3Service.upload("ekstep-dev-data-store", "src/test/resources/test-extract.zip", "testUpload/test-extract.zip")
        s3Service.copyObjects("ekstep-dev-data-store", "testUpload/test-extract.zip", "ekstep-dev-data-store", "testDuplicate/test-extract.zip")

        s3Service.extractArchive("ekstep-dev-data-store", "testUpload/test-extract.zip", "testUpload/test-extract/")

        s3Service.closeContext()
    }
}
