package org.sunbird.cloud.storage.service.oci;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.sunbird.cloud.storage.BaseStorageServiceIntegrationTest;
import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Oracle Cloud Infrastructure Object Storage integration tests.
 *
 * <p>Tests are skipped by default and only execute when the following environment
 * variables are set:</p>
 * <ul>
 *   <li>{@code OCI_STORAGE_TEST_ENABLED} — any non-null value enables the tests</li>
 *   <li>{@code OCI_STORAGE_TEST_CONTAINER} — name of a pre-existing, writable OCI bucket</li>
 * </ul>
 *
 * <p>Authentication is determined by {@code OCI_STORAGE_TEST_AUTH_TYPE}:</p>
 * <ul>
 *   <li>{@code ACCESS_KEY} (default) — API Key auth; requires:
 *     <ul>
 *       <li>{@code OCI_TENANCY_ID} — tenancy OCID</li>
 *       <li>{@code OCI_USER_ID} — user OCID (mapped to storageKey)</li>
 *       <li>{@code OCI_FINGERPRINT} — key fingerprint</li>
 *       <li>{@code OCI_PRIVATE_KEY_PEM} — PEM content of the private key (mapped to storageSecret)</li>
 *       <li>{@code OCI_REGION} — OCI region identifier (e.g. {@code us-ashburn-1})</li>
 *     </ul>
 *   </li>
 *   <li>{@code INSTANCE_PROFILE} / {@code IAM} — uses Instance Principals
 *       (for OCI Compute instances or OKE pods)</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OciStorageServiceTest extends BaseStorageServiceIntegrationTest {

    @Override
    protected IStorageService createService() {
        assumeTrue(System.getenv("OCI_STORAGE_TEST_ENABLED") != null,
                "Skipping OCI integration tests: set OCI_STORAGE_TEST_ENABLED=true to enable");

        String authTypeStr = getEnv("OCI_STORAGE_TEST_AUTH_TYPE", "ACCESS_KEY");
        StorageConfig.AuthType authType = StorageConfig.AuthType.valueOf(authTypeStr.toUpperCase());

        StorageConfig.Builder builder = StorageConfig.builder(StorageConfig.StorageType.OCI)
                .authType(authType)
                .region(getEnv("OCI_REGION", "us-ashburn-1"));

        if (authType == StorageConfig.AuthType.ACCESS_KEY) {
            String userId = System.getenv("OCI_USER_ID");
            String privateKeyPem = System.getenv("OCI_PRIVATE_KEY_PEM");

            assumeTrue(userId != null && !userId.isBlank(),
                    "Skipping: OCI_USER_ID must be set for ACCESS_KEY auth");
            assumeTrue(privateKeyPem != null && !privateKeyPem.isBlank(),
                    "Skipping: OCI_PRIVATE_KEY_PEM must contain the private key PEM for ACCESS_KEY auth");
            assumeTrue(System.getenv("OCI_TENANCY_ID") != null,
                    "Skipping: OCI_TENANCY_ID must be set for ACCESS_KEY auth");
            assumeTrue(System.getenv("OCI_FINGERPRINT") != null,
                    "Skipping: OCI_FINGERPRINT must be set for ACCESS_KEY auth");

            // storageKey = user OCID, storageSecret = PEM content (as expected by OciStorageService)
            builder.storageKey(userId).storageSecret(privateKeyPem);
        }

        return new OciStorageService(builder.build());
    }

    @Override
    protected String getContainer() {
        String container = System.getenv("OCI_STORAGE_TEST_CONTAINER");
        assumeTrue(container != null && !container.isBlank(),
                "Skipping: OCI_STORAGE_TEST_CONTAINER must specify a pre-existing writable bucket");
        return container;
    }

    @BeforeAll
    static void checkOciEnabled() {
        assumeTrue(System.getenv("OCI_STORAGE_TEST_ENABLED") != null,
                "Skipping OCI integration tests: set OCI_STORAGE_TEST_ENABLED=true to enable");
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
