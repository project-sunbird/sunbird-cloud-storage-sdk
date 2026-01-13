package org.sunbird.cloud.storage.service

import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.util.IAMCredentialsSupplier
import java.util.Properties

/**
  * S3 Storage Service that uses IAM Role-based authentication.
  * Uses AWS SDK v2 DefaultCredentialsProvider to resolve credentials from:
  * - EC2 Instance Profile
  * - ECS Task Role
  * - EKS IRSA (IAM Roles for Service Accounts)
  * - Environment variables
  * - AWS credentials file
  */
class S3IAMStorageService(config: StorageConfig) extends BaseStorageService {

    // Region configuration (recommended for IAM auth)
    private val awsRegion = config.region.filter(_.nonEmpty).getOrElse("us-east-1")

    // Configure region via JClouds properties
    private val overrides = new Properties()
    overrides.setProperty("jclouds.regions", awsRegion)

    // Use credentialsSupplier for dynamic IAM credential resolution
    var context = ContextBuilder.newBuilder("aws-s3")
        .credentialsSupplier(new IAMCredentialsSupplier())
        .overrides(overrides)
        .buildView(classOf[BlobStoreContext])

    var blobStore = context.getBlobStore

    override def getPaths(container: String, objects: List[Blob]): List[String] = {
        objects.map(f => "s3n://" + container + "/" + f.key)
    }
}
