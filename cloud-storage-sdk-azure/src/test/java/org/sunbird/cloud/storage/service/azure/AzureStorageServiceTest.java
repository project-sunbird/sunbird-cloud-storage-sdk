package org.sunbird.cloud.storage.service.azure;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.sunbird.cloud.storage.BaseStorageServiceIntegrationTest;
import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Azure Blob Storage integration tests.
 *
 * <p>Tests are skipped by default and only execute when the following environment
 * variables are set:</p>
 * <ul>
 *   <li>{@code AZURE_STORAGE_TEST_ENABLED} — any non-null value enables the tests</li>
 *   <li>{@code AZURE_STORAGE_TEST_CONTAINER} — name of a pre-existing, writable container</li>
 *   <li>{@code AZURE_STORAGE_ACCOUNT_NAME} — storage account name</li>
 * </ul>
 *
 * <p>Authentication is determined by {@code AZURE_STORAGE_TEST_AUTH_TYPE}:</p>
 * <ul>
 *   <li>{@code ACCESS_KEY} (default) — reads {@code AZURE_STORAGE_ACCOUNT_KEY}</li>
 *   <li>{@code OIDC} — uses Workload Identity; requires {@code AZURE_CLIENT_ID},
 *       {@code AZURE_TENANT_ID}, {@code AZURE_FEDERATED_TOKEN_FILE} or falls back to
 *       {@code DefaultAzureCredential}</li>
 *   <li>{@code IAM} / {@code INSTANCE_PROFILE} — uses Managed Identity</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AzureStorageServiceTest extends BaseStorageServiceIntegrationTest {

    @Override
    protected IStorageService createService() {
        assumeTrue(System.getenv("AZURE_STORAGE_TEST_ENABLED") != null,
                "Skipping Azure integration tests: set AZURE_STORAGE_TEST_ENABLED=true to enable");

        String accountName = System.getenv("AZURE_STORAGE_ACCOUNT_NAME");
        assumeTrue(accountName != null && !accountName.isBlank(),
                "Skipping: AZURE_STORAGE_ACCOUNT_NAME must be set");

        String authTypeStr = getEnv("AZURE_STORAGE_TEST_AUTH_TYPE", "ACCESS_KEY");
        StorageConfig.AuthType authType = StorageConfig.AuthType.valueOf(authTypeStr.toUpperCase());

        StorageConfig.Builder builder = StorageConfig.builder(StorageConfig.StorageType.AZURE)
                .authType(authType)
                .storageKey(accountName);

        if (authType == StorageConfig.AuthType.ACCESS_KEY) {
            String accountKey = System.getenv("AZURE_STORAGE_ACCOUNT_KEY");
            assumeTrue(accountKey != null && !accountKey.isBlank(),
                    "Skipping: AZURE_STORAGE_ACCOUNT_KEY must be set for ACCESS_KEY auth");
            builder.storageSecret(accountKey);
        }

        return new AzureStorageService(builder.build());
    }

    @Override
    protected String getContainer() {
        String container = System.getenv("AZURE_STORAGE_TEST_CONTAINER");
        assumeTrue(container != null && !container.isBlank(),
                "Skipping: AZURE_STORAGE_TEST_CONTAINER must specify a pre-existing writable container");
        return container;
    }

    @BeforeAll
    static void checkAzureEnabled() {
        assumeTrue(System.getenv("AZURE_STORAGE_TEST_ENABLED") != null,
                "Skipping Azure integration tests: set AZURE_STORAGE_TEST_ENABLED=true to enable");
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
