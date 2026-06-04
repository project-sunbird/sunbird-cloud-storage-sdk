package org.sunbird.cloud.storage;

/**
 * Configuration for cloud storage service initialization.
 * Use the {@link Builder} pattern for construction.
 */
public final class StorageConfig {

    public enum AuthType {
        ACCESS_KEY,
        OIDC,
        IAM,
        IAM_ROLE,
        INSTANCE_PROFILE
    }

    public enum StorageType {
        AWS, AZURE, GCLOUD, OCI, CEPHS3
    }

    private final StorageType type;
    private final String storageKey;
    private final String storageSecret;
    private final String endPoint;
    private final String region;
    private final AuthType authType;

    private StorageConfig(Builder builder) {
        this.type = builder.type;
        this.storageKey = builder.storageKey;
        this.storageSecret = builder.storageSecret;
        this.endPoint = builder.endPoint;
        this.region = builder.region;
        this.authType = builder.authType;
    }

    public StorageType getType() {
        return type;
    }

    public String getStorageKey() {
        return storageKey;
    }

    public String getStorageSecret() {
        return storageSecret;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public String getRegion() {
        return region;
    }

    public AuthType getAuthType() {
        return authType;
    }

    public static Builder builder(StorageType type) {
        return new Builder(type);
    }

    public static class Builder {
        private final StorageType type;
        private String storageKey = "";
        private String storageSecret = "";
        private String endPoint;
        private String region;
        private AuthType authType = AuthType.ACCESS_KEY;

        private Builder(StorageType type) {
            this.type = type;
        }

        public Builder storageKey(String storageKey) {
            this.storageKey = storageKey;
            return this;
        }

        public Builder storageSecret(String storageSecret) {
            this.storageSecret = storageSecret;
            return this;
        }

        public Builder endPoint(String endPoint) {
            this.endPoint = endPoint;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder authType(AuthType authType) {
            this.authType = authType;
            return this;
        }

        public StorageConfig build() {
            return new StorageConfig(this);
        }
    }
}
