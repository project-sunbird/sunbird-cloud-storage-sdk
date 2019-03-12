package org.sunbird.cloud.storage.service

import org.scalatest.FlatSpec
import org.scalatest._
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

class TestS3StorageService extends FlatSpec with Matchers {

    it should "test for s3 storage" in {

        val s3Service = StorageServiceFactory.getStorageService(StorageConfig("aws", AppConf.getStorageKey("aws"), AppConf.getStorageSecret("aws")))

        val storageContainer = AppConf.getConfig("aws_storage_container")

        //println(s3Service.upload(storageContainer, "src/test/resources/110Mb_File.zip", "testUpload/110Mb_File.zip", Option(false),Option(false),None, Option(3), 1))
        val caught =
            intercept[StorageServiceException]{
                s3Service.upload(storageContainer, "src/test/resources/1234/test-blob.log", "testUpload/1234/", Option(false),Option(5), Option(2), None)
            }
        assert(caught.getMessage.contains("Failed to upload."))
        /*
        s3Service.upload(storageContainer, "src/test/resources/test-data.log", "testUpload/test-blob.log")
        s3Service.download(storageContainer, "testUpload/test-blob.log", "src/test/resources/test-s3/")

        // upload directory
        println("url of folder", s3Service.upload(storageContainer, "src/test/resources/1234/", "testUpload/1234/", None, Option(true)))

        // downlaod directory
        s3Service.download(storageContainer, "testUpload/1234/", "src/test/resources/test-s3/", Option(true))

        println("azure signed url", s3Service.getSignedURL(storageContainer, "testUpload/test-blob.log", Option(600)))

        val blob = s3Service.getObject(storageContainer, "testUpload/test-blob.log")
        println("blob details: ", blob)

        println("upload public url", s3Service.upload(storageContainer, "src/test/resources/test-data.log", "testUpload/test-data-public.log", Option(true)))
        println("upload public with expiry url", s3Service.upload(storageContainer, "src/test/resources/test-data.log", "testUpload/test-data-with-expiry.log", Option(true), Option(false), Option(600)))
        println("signed path to upload from external client", s3Service.getSignedURL(storageContainer, "testUpload/test-data-public1.log", Option(600), Option("w")))

        val keys = s3Service.searchObjectkeys(storageContainer, "testUpload/1234/")
        keys.foreach(f => println(f))
        val blobs = s3Service.searchObjects(storageContainer, "testUpload/1234/")
        blobs.foreach(f => println(f))

        val objData = s3Service.getObjectData(storageContainer, "testUpload/test-blob.log")
        objData.length should be(18)

        // delete directory
        s3Service.deleteObject(storageContainer, "testUpload/1234/", Option(true))
        s3Service.deleteObject(storageContainer, "testUpload/test-blob.log")
        //s3Service.deleteObject(storageContainer, "testUpload/test-data-public.log")
        //s3Service.deleteObject(storageContainer, "testUpload/test-data-with-expiry.log")

        s3Service.upload(storageContainer, "src/test/resources/test-extract.zip", "testUpload/test-extract.zip")
        s3Service.copyObjects(storageContainer, "testUpload/test-extract.zip", storageContainer, "testDuplicate/test-extract.zip")

        s3Service.extractArchive(storageContainer, "testUpload/test-extract.zip", "testUpload/test-extract/")
        */

        // Test if the listObjectKeys lists all objects inside a directory when the number of objects count
        // is greater than threshold value 1000
       /* val objectsListInsideADirectory = s3Service.listObjectKeys("ekstep-public-dev", _prefix = "content/h5p/do_112620111166709760183-snapshot/content")
        objectsListInsideADirectory.size should be (1200)

        // Test if the listObjectKeys lists individual files if the prefix is not a directory
        val fileObject = s3Service.listObjectKeys("ekstep-public-dev", _prefix = "content/h5p/do_112620111166709760183-snapshot/index.html")
        fileObject.size should be (1)
        fileObject.head should be ("content/h5p/do_112620111166709760183-snapshot/index.html")

        // Test if the listObjectKeys lists no files if the filename is not found
        val noFilesFound = s3Service.listObjectKeys("ekstep-public-dev", _prefix = "random_file_name_which_is_not_present")
        noFilesFound.size should be (0)*/

        s3Service.closeContext()
    }
}
