package org.sunbird.cloud.storage.service

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import scala.io.Source
import org.sunbird.cloud.storage.util.JSONUtils

class TestGcloudStorageServiceGetPutSignedURL extends FlatSpec with Matchers {

  ignore should "return a valid V4 PUT signed URL for GCP" in {

    // 1) Compose service via factory
    val gsService = StorageServiceFactory.getStorageService(StorageConfig("gcloud", AppConf.getStorageKey, AppConf.getStorageSecret))

    val storageContainer = AppConf.getConfig("gcloud_storage_container")
    // val objectKey = s"testUpload/test-put-signed-${System.currentTimeMillis()}.log"
    val objectKey = s"testUpload/lesffichage.mp4"

    // 2) Build additionalParams from gcs-creds.json using JSONUtils
    val credsPath = "src/test/resources/gcs-creds.json"
    val credsJson = Source.fromFile(credsPath).getLines().mkString
    val creds: Map[String, Any] = JSONUtils.deserialize[Map[String, Any]](credsJson)

    def str(key: String): String = creds.get(key).map(_.toString).getOrElse("")

    val privateKeyFixed = str("private_key").replace("\\n", "\n")

    val additionalParams: Map[String, String] = Map(
      "clientId" -> str("client_id"),
      "clientEmail" -> str("client_email"),
      "privateKeyPkcs8" -> privateKeyFixed,
      "privateKeyIds" -> str("private_key_id"),
      "projectId" -> str("project_id"),
      "chunked" -> "true"
    )

    // Call getPutSignedURL with write permission and a contentType
    val ttlSeconds = Option(86400)
    val contentType = Option("video/mp4")
    val url = gsService.getPutSignedURL(storageContainer, objectKey, ttlSeconds, Option("w"), contentType, Option(additionalParams))
    println("Put Signed URL: " + url)

    // Basic validity assertions
    url should include ("https://storage.googleapis.com/")
    url should include (storageContainer)
    url should include (objectKey)
    url.toLowerCase should include ("x-goog-signature")

    gsService.closeContext()
  }
}
