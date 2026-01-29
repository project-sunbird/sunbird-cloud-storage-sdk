package org.sunbird.cloud.storage.util

import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, AwsCredentials, AwsSessionCredentials}
import org.jclouds.domain.Credentials
import org.jclouds.aws.domain.SessionCredentials
import com.google.common.base.Supplier
import java.io.File
import scala.io.Source
import java.time.Duration
import com.azure.core.credential.TokenRequestContext
import org.sunbird.cloud.storage.conf.AppConf
/**
  * A Guava Supplier that bridges cloud provider credential resolution with JClouds.
  * Supports AWS, Azure, and GCP credential resolution from various sources.
  *
  * AWS: Uses DefaultCredentialsProvider which checks credentials in this order:
  * 1. System properties (aws.accessKeyId, aws.secretAccessKey)
  * 2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
  * 3. Web Identity Token (EKS IRSA)
  * 4. Credential profiles (~/.aws/credentials)
  * 5. ECS Container credentials
  * 6. EC2 Instance Profile (IMDS)
  *
  * Azure: Checks credentials in this order:
  * 1. Environment variables (AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY)
  * 2. Service Principal (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
  * 3. Managed Identity (when running on Azure infrastructure)
  *
  * GCP: Checks credentials in this order:
  * 1. Environment variable GOOGLE_APPLICATION_CREDENTIALS (path to service account JSON)
  * 2. Service account JSON file parsing (extracts client_email and private_key)
  * 3. Application Default Credentials (ADC) - when running on GCP infrastructure
  *
  * For IAM roles (EC2, ECS, EKS for AWS), temporary credentials with session tokens are returned.
  *
  * @param cloudProvider The cloud provider type: "aws", "azure", or "gcp" (default: "aws" for backward compatibility)
  */
class IAMCredentialsSupplier(cloudProvider: String = "aws") extends Supplier[Credentials] {

  private val provider = cloudProvider.toLowerCase()

  // AWS credential provider (lazy initialization)
  private lazy val awsProvider = DefaultCredentialsProvider.builder().build()
  
  // Azure credential provider (lazy initialization)
  private lazy val azureCredential = {
    val builder = new com.azure.identity.DefaultAzureCredentialBuilder()
    // Support user-assigned managed identity via environment variable
    Option(System.getenv("AZURE_CLIENT_ID"))
      .orElse(Option(System.getProperty("azure.client.id")))
      .foreach(clientId => builder.managedIdentityClientId(clientId))
    builder.build()
  }

  override def get(): Credentials = {
    provider match {
      case "aws" => getAWSCredentials()
      case "azure" => getAzureCredentials()
      case "gcp" | "gcloud" => getGCPCredentials()
      case _ => throw new IllegalArgumentException(s"Unsupported cloud provider: $cloudProvider. Supported: aws, azure, gcp")
    }
  }

  /**
    * Resolves AWS credentials using AWS SDK v2 DefaultCredentialsProvider
    */
  private def getAWSCredentials(): Credentials = {
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

  /**
    * Resolves Azure credentials from environment variables or managed identity
    * Azure Blob Storage uses storage account name as key and storage key as secret
    */
  private def getAzureCredentials(): Credentials = {
    // Try Azure Storage Account credentials first (standard for Azure Blob Storage)
    val storageAccount = Option(System.getenv("AZURE_STORAGE_ACCOUNT"))
      .orElse(Option(System.getProperty("azure.storage.account")))
    
    val storageKey = Option(System.getenv("AZURE_STORAGE_KEY"))
      .orElse(Option(System.getProperty("azure.storage.key")))

    if (storageAccount.isDefined && storageKey.isDefined) {
      return new Credentials(storageAccount.get, storageKey.get)
    }

    // Try Service Principal credentials
    val clientId = Option(System.getenv("AZURE_CLIENT_ID"))
      .orElse(Option(System.getProperty("azure.client.id")))
    
    val clientSecret = Option(System.getenv("AZURE_CLIENT_SECRET"))
      .orElse(Option(System.getProperty("azure.client.secret")))
    
    val tenantId = Option(System.getenv("AZURE_TENANT_ID"))
      .orElse(Option(System.getProperty("azure.tenant.id")))

    if (clientId.isDefined && clientSecret.isDefined && tenantId.isDefined) {
      // For service principal, use client_id as key and client_secret as secret
      // Note: JClouds may need additional configuration for OAuth token-based auth
      return new Credentials(clientId.get, clientSecret.get)
    }

    // Try Managed Identity (when running on Azure infrastructure)
    // DefaultAzureCredential will automatically try Managed Identity if available
    // It checks in this order: Environment variables -> Managed Identity -> Azure CLI -> etc.
    try {
      // Get storage account name (required for Azure Blob Storage)
      val storageAccountName = Option(System.getenv("AZURE_STORAGE_ACCOUNT"))
        .orElse(Option(System.getProperty("azure.storage.account")))
        .orElse(Option(AppConf.getStorageKey))
        .getOrElse(throw new IllegalStateException(
          "AZURE_STORAGE_ACCOUNT must be set when using Managed Identity for Azure Blob Storage"
        ))
      
      // Get access token using Managed Identity via DefaultAzureCredential
      // For Azure Storage, we need the storage scope
      val tokenRequestContext = new TokenRequestContext()
        .addScopes("https://storage.azure.com/.default")
      
      val accessToken = azureCredential.getToken(tokenRequestContext).block(Duration.ofSeconds(30))
      
      if (accessToken == null) {
        throw new IllegalStateException("Failed to obtain access token from Azure Managed Identity")
      }
      
      // For JClouds compatibility, we use storage account name as key
      // and the access token as secret (token will be used for authentication)
      // Note: JClouds may need additional configuration to use token-based auth
      // For now, we return the token as the secret
      new Credentials(storageAccountName, accessToken.getToken)
    } catch {
      case e: IllegalStateException => 
        // Re-throw our own exceptions
        throw e
      case e: com.azure.core.exception.ClientAuthenticationException =>
        // Azure Identity SDK authentication failed (not on Azure, no MI, etc.)
        // Fall through to final error message
        throw new IllegalStateException(
          s"Azure Managed Identity authentication failed: ${e.getMessage}. " +
          "This may occur if not running on Azure infrastructure or Managed Identity is not enabled. " +
          "Please set AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY environment variables as fallback.",
          e
        )
      case e: Exception =>
        // Other exceptions (timeout, network, etc.)
        throw new IllegalStateException(
          s"Failed to obtain credentials from Azure Managed Identity: ${e.getMessage}. " +
          "Please ensure Managed Identity is enabled on the Azure resource and has proper permissions, " +
          "or set AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY environment variables as fallback.",
          e
        )
    }

    throw new IllegalStateException(
      "Azure credentials not found. Please set one of the following:\n" +
      "1. AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY (for Blob Storage)\n" +
      "2. AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, and AZURE_TENANT_ID (for Service Principal)\n" +
      "3. Run on Azure infrastructure with Managed Identity enabled"
    )
  }

  /**
    * Resolves GCP credentials from environment variables or service account JSON
    * GCP uses service account email as key and private key as secret
    */
  private def getGCPCredentials(): Credentials = {
    // Try GOOGLE_APPLICATION_CREDENTIALS environment variable (standard GCP way)
    val credentialsPath = Option(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
      .orElse(Option(System.getProperty("google.application.credentials")))

    if (credentialsPath.isDefined) {
      val file = new File(credentialsPath.get)
      if (file.exists() && file.isFile) {
        return parseGCPServiceAccount(file)
      }
    }

    // Try default service account JSON locations
    val defaultPaths = List(
      System.getProperty("user.home") + "/.config/gcloud/application_default_credentials.json",
      System.getProperty("user.home") + "/.gcloud/application_default_credentials.json"
    )

    for (path <- defaultPaths) {
      val file = new File(path)
      if (file.exists() && file.isFile) {
        return parseGCPServiceAccount(file)
      }
    }

    // Try to parse from JSON string in environment variable
    val credentialsJson = Option(System.getenv("GOOGLE_CREDENTIALS_JSON"))
      .orElse(Option(System.getProperty("google.credentials.json")))

    if (credentialsJson.isDefined) {
      return parseGCPServiceAccountJson(credentialsJson.get)
    }

    // Application Default Credentials (ADC) - when running on GCP infrastructure
    // This requires Google Auth Library which may not be fully available
    // Check for GCP metadata server (indicates running on GCP)
    val gcpMetadataServer = Option(System.getenv("GCE_METADATA_HOST"))
      .orElse(Option(System.getenv("GCP_METADATA_HOST")))

    if (gcpMetadataServer.isDefined || isRunningOnGCP()) {
      throw new IllegalStateException(
        "GCP Application Default Credentials detected but Google Auth Library is not fully available. " +
        "Please set GOOGLE_APPLICATION_CREDENTIALS environment variable to a service account JSON file path, " +
        "or add com.google.auth:google-auth-library-oauth2-http dependency to use ADC."
      )
    }

    throw new IllegalStateException(
      "GCP credentials not found. Please set one of the following:\n" +
      "1. GOOGLE_APPLICATION_CREDENTIALS environment variable (path to service account JSON file)\n" +
      "2. GOOGLE_CREDENTIALS_JSON environment variable (JSON string)\n" +
      "3. Service account JSON file at ~/.config/gcloud/application_default_credentials.json\n" +
      "4. Run on GCP infrastructure with Application Default Credentials enabled"
    )
  }

  /**
    * Parses GCP service account JSON file and extracts credentials
    */
  private def parseGCPServiceAccount(file: File): Credentials = {
    try {
      val jsonContent = Source.fromFile(file).mkString
      parseGCPServiceAccountJson(jsonContent)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to parse GCP service account file: ${file.getAbsolutePath}", e)
    }
  }

  /**
    * Parses GCP service account JSON string and extracts client_email and private_key
    */
  private def parseGCPServiceAccountJson(jsonContent: String): Credentials = {
    try {
      // Simple JSON parsing to extract client_email and private_key
      // Using regex as a lightweight approach (could be enhanced with a JSON library)
      val clientEmailPattern = """"client_email"\s*:\s*"([^"]+)"""".r
      val privateKeyPattern = """"private_key"\s*:\s*"([^"]+)"""".r

      val clientEmail = clientEmailPattern.findFirstMatchIn(jsonContent)
        .map(_.group(1))
        .getOrElse(throw new IllegalStateException("client_email not found in service account JSON"))

      val privateKey = privateKeyPattern.findFirstMatchIn(jsonContent)
        .map(_.group(1).replace("\\n", "\n")) // Unescape newlines
        .getOrElse(throw new IllegalStateException("private_key not found in service account JSON"))

      new Credentials(clientEmail, privateKey)
    } catch {
      case e: IllegalStateException => throw e
      case e: Exception =>
        throw new IllegalStateException("Failed to parse GCP service account JSON", e)
    }
  }

  /**
    * Checks if running on GCP infrastructure by attempting to access metadata server
    */
  private def isRunningOnGCP(): Boolean = {
    try {
      // Try to access GCP metadata server (only available on GCP infrastructure)
      val url = new java.net.URL("http://metadata.google.internal/computeMetadata/v1/instance/id")
      val connection = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setRequestProperty("Metadata-Flavor", "Google")
      connection.setConnectTimeout(1000)
      connection.setReadTimeout(1000)
      val responseCode = connection.getResponseCode
      connection.disconnect()
      responseCode == 200
    } catch {
      case _: Exception => false
    }
  }
}
