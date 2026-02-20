package org.sunbird.cloud.storage.service.aws;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.sunbird.cloud.storage.BaseStorageServiceIntegrationTest;
import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * AWS S3 integration tests.
 *
 * <p>Tests are skipped by default and only execute when the following environment
 * variables are set:</p>
 * <ul>
 *   <li>{@code AWS_STORAGE_TEST_ENABLED} — any non-null value enables the tests</li>
 *   <li>{@code AWS_STORAGE_TEST_CONTAINER} — name of a pre-existing, writable S3 bucket</li>
 * </ul>
 *
 * <p>Authentication is determined by {@code AWS_STORAGE_TEST_AUTH_TYPE}:</p>
 * <ul>
 *   <li>{@code ACCESS_KEY} (default) — reads {@code AWS_ACCESS_KEY_ID} and
 *       {@code AWS_SECRET_ACCESS_KEY}</li>
 *   <li>{@code IAM} / {@code IAM_ROLE} / {@code INSTANCE_PROFILE} — uses the
 *       default AWS credential chain (instance profile, task role, etc.)</li>
 * </ul>
 *
 * <p>Optional: {@code AWS_REGION} (default: {@code us-east-1})</p>
 *
 * <p>For Ceph S3-compatible endpoints, also set:</p>
 * <ul>
 *   <li>{@code AWS_STORAGE_TEST_ENDPOINT} — custom endpoint URL</li>
 *   <li>{@code AWS_STORAGE_TEST_TYPE=CEPHS3}</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsStorageServiceTest extends BaseStorageServiceIntegrationTest {

    @Override
    protected IStorageService createService() {
        assumeTrue(System.getenv("AWS_STORAGE_TEST_ENABLED") != null,
                "Skipping AWS integration tests: set AWS_STORAGE_TEST_ENABLED=true to enable");

        String authTypeStr = getEnv("AWS_STORAGE_TEST_AUTH_TYPE", "ACCESS_KEY");
        StorageConfig.AuthType authType = StorageConfig.AuthType.valueOf(authTypeStr.toUpperCase());

        String storageTypeStr = getEnv("AWS_STORAGE_TEST_TYPE", "AWS");
        StorageConfig.StorageType storageType = StorageConfig.StorageType.valueOf(storageTypeStr.toUpperCase());

        StorageConfig.Builder builder = StorageConfig.builder(storageType)
                .authType(authType)
                .region(getEnv("AWS_REGION", "us-east-1"));

        if (authType == StorageConfig.AuthType.ACCESS_KEY) {
            String key = System.getenv("AWS_ACCESS_KEY_ID");
            String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
            assumeTrue(key != null && secret != null,
                    "Skipping: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set for ACCESS_KEY auth");
            builder.storageKey(key).storageSecret(secret);
        }

        String endpoint = System.getenv("AWS_STORAGE_TEST_ENDPOINT");
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endPoint(endpoint);
        }

        return new AwsStorageService(builder.build());
    }

    @Override
    protected String getContainer() {
        String container = System.getenv("AWS_STORAGE_TEST_CONTAINER");
        assumeTrue(container != null && !container.isBlank(),
                "Skipping: AWS_STORAGE_TEST_CONTAINER must specify a pre-existing writable bucket");
        return container;
    }

    @BeforeAll
    static void checkAwsEnabled() {
        assumeTrue(System.getenv("AWS_STORAGE_TEST_ENABLED") != null,
                "Skipping AWS integration tests: set AWS_STORAGE_TEST_ENABLED=true to enable");
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
