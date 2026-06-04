package org.sunbird.cloud.storage.service.gcp;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.sunbird.cloud.storage.BaseStorageServiceIntegrationTest;
import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Google Cloud Storage integration tests.
 *
 * <p>Tests are skipped by default and only execute when the following environment
 * variables are set:</p>
 * <ul>
 *   <li>{@code GCP_STORAGE_TEST_ENABLED} — any non-null value enables the tests</li>
 *   <li>{@code GCP_STORAGE_TEST_CONTAINER} — name of a pre-existing, writable GCS bucket</li>
 * </ul>
 *
 * <p>Authentication is determined by {@code GCP_STORAGE_TEST_AUTH_TYPE}:</p>
 * <ul>
 *   <li>{@code ACCESS_KEY} (default) — reads {@code GCP_SERVICE_ACCOUNT_KEY_JSON}
 *       (the full JSON content of a service account key file)</li>
 *   <li>{@code OIDC} / {@code IAM} — uses Application Default Credentials
 *       ({@code GOOGLE_APPLICATION_CREDENTIALS} or GCE metadata)</li>
 * </ul>
 *
 * <p>Optional: {@code GCP_STORAGE_TEST_PROJECT} — GCP project ID (used for some operations)</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GcpStorageServiceTest extends BaseStorageServiceIntegrationTest {

    @Override
    protected IStorageService createService() {
        assumeTrue(System.getenv("GCP_STORAGE_TEST_ENABLED") != null,
                "Skipping GCP integration tests: set GCP_STORAGE_TEST_ENABLED=true to enable");

        String authTypeStr = getEnv("GCP_STORAGE_TEST_AUTH_TYPE", "ACCESS_KEY");
        StorageConfig.AuthType authType = StorageConfig.AuthType.valueOf(authTypeStr.toUpperCase());

        StorageConfig.Builder builder = StorageConfig.builder(StorageConfig.StorageType.GCLOUD)
                .authType(authType);

        if (authType == StorageConfig.AuthType.ACCESS_KEY) {
            String serviceAccountJson = System.getenv("GCP_SERVICE_ACCOUNT_KEY_JSON");
            assumeTrue(serviceAccountJson != null && !serviceAccountJson.isBlank(),
                    "Skipping: GCP_SERVICE_ACCOUNT_KEY_JSON must contain the service account key JSON for ACCESS_KEY auth");
            builder.storageSecret(serviceAccountJson);
        }

        String project = System.getenv("GCP_STORAGE_TEST_PROJECT");
        if (project != null && !project.isBlank()) {
            builder.storageKey(project);
        }

        return new GcpStorageService(builder.build());
    }

    @Override
    protected String getContainer() {
        String container = System.getenv("GCP_STORAGE_TEST_CONTAINER");
        assumeTrue(container != null && !container.isBlank(),
                "Skipping: GCP_STORAGE_TEST_CONTAINER must specify a pre-existing writable GCS bucket");
        return container;
    }

    @BeforeAll
    static void checkGcpEnabled() {
        assumeTrue(System.getenv("GCP_STORAGE_TEST_ENABLED") != null,
                "Skipping GCP integration tests: set GCP_STORAGE_TEST_ENABLED=true to enable");
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
