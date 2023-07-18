package org.sunbird.cloud.storage.conf

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

object AppConf {

    lazy val defaultConf = ConfigFactory.load();
    lazy val envConf = ConfigFactory.systemEnvironment();
    lazy val conf = envConf.withFallback(defaultConf);

    def getConfig(key: String): String = {
        if (conf.hasPath(key))
            conf.getString(key);
        else "";
    }

    def getConfig: Config = conf


    def getStorageType: String = getConfig("cloud_storage_type")

    def getStorageKey: String = getConfig("cloud_storage_key")

    def getStorageSecret: String = getConfig("cloud_storage_secret")

    def getRegion: Option[String] = {
        if (getStorageType.equals("oci"))
            Option(getConfig("cloud_storage_region"))
        else Option("")
    }

    def getEndPoint: Option[String] = {
        if (getStorageType.equals("oci"))
            Option(getConfig("cloud_storage_endpoint"))
        else Option("")
    }
}
