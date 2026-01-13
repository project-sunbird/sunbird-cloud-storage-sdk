package org.sunbird.cloud.storage.util

import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, AwsCredentials, AwsSessionCredentials}
import org.jclouds.domain.Credentials
import org.jclouds.aws.domain.SessionCredentials
import com.google.common.base.Supplier

/**
  * A Guava Supplier that bridges AWS SDK v2 credential resolution with JClouds.
  * Uses DefaultCredentialsProvider which checks credentials in this order:
  * 1. System properties (aws.accessKeyId, aws.secretAccessKey)
  * 2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
  * 3. Web Identity Token (EKS IRSA)
  * 4. Credential profiles (~/.aws/credentials)
  * 5. ECS Container credentials
  * 6. EC2 Instance Profile (IMDS)
  *
  * For IAM roles (EC2, ECS, EKS), temporary credentials with session tokens are returned.
  */
class IAMCredentialsSupplier extends Supplier[Credentials] {

  private val awsProvider = DefaultCredentialsProvider.builder().build()

  override def get(): Credentials = {
    val awsCreds: AwsCredentials = awsProvider.resolveCredentials()

    awsCreds match {
      case sessionCreds: AwsSessionCredentials =>
        // IAM role credentials include a session token that must be passed to JClouds
        SessionCredentials.builder()
          .accessKeyId(sessionCreds.accessKeyId())
          .secretAccessKey(sessionCreds.secretAccessKey())
          .sessionToken(sessionCreds.sessionToken())
          .build()
      case _ =>
        // Static credentials (from env vars, config file, etc.)
        new Credentials(awsCreds.accessKeyId(), awsCreds.secretAccessKey())
    }
  }
}
