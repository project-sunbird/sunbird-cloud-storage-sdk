package org.sunbird.cloud.storage.model;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Represents a cloud storage blob object with metadata and optional payload.
 */
public final class Blob {

    private final String key;
    private final long contentLength;
    private final Date lastModified;
    private final Map<String, Object> metadata;
    private final byte[] payload;

    public Blob(String key, long contentLength, Date lastModified,
                Map<String, Object> metadata, byte[] payload) {
        this.key = key;
        this.contentLength = contentLength;
        this.lastModified = lastModified;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.payload = payload;
    }

    public Blob(String key, long contentLength, Date lastModified, Map<String, Object> metadata) {
        this(key, contentLength, lastModified, metadata, null);
    }

    public String getKey() {
        return key;
    }

    public long getContentLength() {
        return contentLength;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Blob{key='" + key + "', contentLength=" + contentLength +
                ", lastModified=" + lastModified + "}";
    }
}
