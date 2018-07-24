package org.sunbird.cloud.storage

import java.util.Date

object Model {
  
    case class Blob(key: String, contentLength: Long, lastModified: Date, metadata: Map[String, AnyRef], payload: Option[Array[Byte]]);
}