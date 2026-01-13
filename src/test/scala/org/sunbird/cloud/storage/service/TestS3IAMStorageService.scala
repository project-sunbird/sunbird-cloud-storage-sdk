package org.sunbird.cloud.storage.service

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

/**
  * Test S3 storage service with IAM role authentication.
  * Run this test on an EC2 instance with IAM role attached.
  *
  * Prerequisites:
  * 1. EC2 instance with IAM role that has S3 read permissions
  * 2. Update 'bucket' variable with your actual S3 bucket name
  * 3. Update 'region' in StorageConfig with your bucket's region
  *
  * Run: mvn test -Dtest=TestS3IAMStorageService
  */
class TestS3IAMStorageService extends AnyFlatSpec with Matchers {

    // TODO: Replace with your actual bucket name and region before running on EC2
    val bucket = "your-test-bucket"
    val region = "us-east-1"

    ignore should "test S3 storage with IAM role authentication" in {
        println("=== Starting IAM Role Authentication Test ===\n")

        // Create service with IAM role auth (no access keys needed)
        val s3Service = StorageServiceFactory.getStorageService(
            StorageConfig("aws", "", "", None, Option(region), Option("iam_role"))
        )

        // Test 1: List objects in bucket
        println("=== Test 1: listObjectKeys ===")
        val keys = s3Service.listObjectKeys(bucket, "")
        println(s"Found ${keys.size} objects in bucket '$bucket'")
        if (keys.nonEmpty) {
            println("First 5 objects:")
            keys.take(5).foreach(k => println(s"  - $k"))
        }

        // Test 2: Get object metadata (if objects exist)
        if (keys.nonEmpty) {
            println("\n=== Test 2: getObject ===")
            val blob = s3Service.getObject(bucket, keys.head)
            println(s"Object key: ${blob.key}")
            println(s"Content length: ${blob.contentLength} bytes")
            println(s"Last modified: ${blob.lastModified}")
        } else {
            println("\n=== Test 2: getObject (skipped - no objects in bucket) ===")
        }

        // Test 3: Search objects with prefix
        println("\n=== Test 3: searchObjectkeys ===")
        val searchResults = s3Service.searchObjectkeys(bucket, "")
        println(s"Search returned ${searchResults.size} objects")

        // Cleanup
        s3Service.closeContext()

        println("\n=== IAM Role Authentication Test COMPLETED ===")
    }
}
