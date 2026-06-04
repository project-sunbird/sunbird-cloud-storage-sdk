# Testing Guide

This document explains how to run tests for the Cloud Storage SDK, including
unit tests that run without any cloud credentials and integration tests that
require access to real cloud storage.

---

## Unit Tests (no credentials required)

The **cloud-storage-sdk-api** module contains pure unit tests that validate
shared logic (date utilities, file utilities, abstract orchestration) without
connecting to any cloud provider. They run with an in-memory storage
implementation.

```bash
# Run API unit tests only
mvn -pl cloud-storage-sdk-api test

# Run all unit tests across all modules (integration tests auto-skip when
# environment variables are absent)
mvn test
```

The unit tests always execute; no special configuration is needed.

---

## Integration Tests

Each CSP module contains an integration test class that exercises the full
storage operations (upload, download, signed URLs, copy, delete, search, etc.)
against a real cloud bucket.

**Integration tests are skipped by default.** They only execute when specific
environment variables are set, making it safe to run `mvn test` in any
environment — CI pipelines, local laptops, or cloud VMs.

### How the skip mechanism works

Each CSP test uses JUnit 5 `assumeTrue()` guards. When the required
`*_STORAGE_TEST_ENABLED` environment variable is absent, JUnit marks
the tests as **skipped** (not failed), so CI pipelines remain green.

---

### AWS S3

**Test class:** `cloud-storage-sdk-aws` → `AwsStorageServiceTest`

| Variable | Required | Description |
|---|---|---|
| `AWS_STORAGE_TEST_ENABLED` | Yes | Any non-empty value (e.g. `true`) enables the tests |
| `AWS_STORAGE_TEST_CONTAINER` | Yes | Name of a pre-existing, writable S3 bucket |
| `AWS_STORAGE_TEST_AUTH_TYPE` | No | `ACCESS_KEY` (default), `IAM`, `IAM_ROLE`, or `INSTANCE_PROFILE` |
| `AWS_ACCESS_KEY_ID` | If ACCESS_KEY | Standard AWS access key |
| `AWS_SECRET_ACCESS_KEY` | If ACCESS_KEY | Standard AWS secret key |
| `AWS_REGION` | No | AWS region (default: `us-east-1`) |
| `AWS_STORAGE_TEST_ENDPOINT` | No | Custom endpoint for Ceph S3-compatible storage |
| `AWS_STORAGE_TEST_TYPE` | No | `AWS` (default) or `CEPHS3` |

#### Example: EC2 instance with IAM role

On an EC2 instance (or EKS pod) that has an IAM role with S3 permissions:

```bash
export AWS_STORAGE_TEST_ENABLED=true
export AWS_STORAGE_TEST_CONTAINER=my-test-bucket
export AWS_STORAGE_TEST_AUTH_TYPE=INSTANCE_PROFILE
export AWS_REGION=us-east-1

mvn -pl cloud-storage-sdk-aws test
```

#### Example: Local development with access keys

```bash
export AWS_STORAGE_TEST_ENABLED=true
export AWS_STORAGE_TEST_CONTAINER=my-test-bucket
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=wJal...
export AWS_REGION=us-east-1

mvn -pl cloud-storage-sdk-aws test
```

#### Example: Ceph S3-compatible storage

```bash
export AWS_STORAGE_TEST_ENABLED=true
export AWS_STORAGE_TEST_CONTAINER=my-ceph-bucket
export AWS_STORAGE_TEST_TYPE=CEPHS3
export AWS_STORAGE_TEST_ENDPOINT=https://ceph-rgw.example.com
export AWS_ACCESS_KEY_ID=ceph-key
export AWS_SECRET_ACCESS_KEY=ceph-secret
export AWS_REGION=us-east-1

mvn -pl cloud-storage-sdk-aws test
```

---

### Azure Blob Storage

**Test class:** `cloud-storage-sdk-azure` → `AzureStorageServiceTest`

| Variable | Required | Description |
|---|---|---|
| `AZURE_STORAGE_TEST_ENABLED` | Yes | Any non-empty value enables the tests |
| `AZURE_STORAGE_TEST_CONTAINER` | Yes | Name of a pre-existing, writable blob container |
| `AZURE_STORAGE_ACCOUNT_NAME` | Yes | Azure Storage account name |
| `AZURE_STORAGE_TEST_AUTH_TYPE` | No | `ACCESS_KEY` (default), `OIDC`, `IAM`, or `INSTANCE_PROFILE` |
| `AZURE_STORAGE_ACCOUNT_KEY` | If ACCESS_KEY | Storage account key |
| `AZURE_CLIENT_ID` | If OIDC | Service principal / managed identity client ID |
| `AZURE_TENANT_ID` | If OIDC | Azure AD tenant ID |
| `AZURE_FEDERATED_TOKEN_FILE` | If OIDC | Path to the federated token file (Kubernetes workload identity) |

#### Example: Azure VM with Managed Identity

On an Azure VM or AKS pod with a system-assigned or user-assigned managed
identity that has `Storage Blob Data Contributor` role:

```bash
export AZURE_STORAGE_TEST_ENABLED=true
export AZURE_STORAGE_TEST_CONTAINER=my-test-container
export AZURE_STORAGE_ACCOUNT_NAME=mystorageaccount
export AZURE_STORAGE_TEST_AUTH_TYPE=IAM

mvn -pl cloud-storage-sdk-azure test
```

#### Example: Local development with account key

```bash
export AZURE_STORAGE_TEST_ENABLED=true
export AZURE_STORAGE_TEST_CONTAINER=my-test-container
export AZURE_STORAGE_ACCOUNT_NAME=mystorageaccount
export AZURE_STORAGE_ACCOUNT_KEY=base64encodedkey...

mvn -pl cloud-storage-sdk-azure test
```

---

### Google Cloud Storage

**Test class:** `cloud-storage-sdk-gcp` → `GcpStorageServiceTest`

| Variable | Required | Description |
|---|---|---|
| `GCP_STORAGE_TEST_ENABLED` | Yes | Any non-empty value enables the tests |
| `GCP_STORAGE_TEST_CONTAINER` | Yes | Name of a pre-existing, writable GCS bucket |
| `GCP_STORAGE_TEST_AUTH_TYPE` | No | `ACCESS_KEY` (default), `OIDC`, or `IAM` |
| `GCP_SERVICE_ACCOUNT_KEY_JSON` | If ACCESS_KEY | Full JSON content of a service account key |
| `GCP_STORAGE_TEST_PROJECT` | No | GCP project ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | If OIDC/IAM | Standard GCP env var pointing to a credentials file |

#### Example: GCE VM with default service account

On a Compute Engine instance (or GKE pod) with Workload Identity or the
default compute service account that has `roles/storage.objectAdmin`:

```bash
export GCP_STORAGE_TEST_ENABLED=true
export GCP_STORAGE_TEST_CONTAINER=my-test-bucket
export GCP_STORAGE_TEST_AUTH_TYPE=IAM

mvn -pl cloud-storage-sdk-gcp test
```

#### Example: Local development with service account key

```bash
export GCP_STORAGE_TEST_ENABLED=true
export GCP_STORAGE_TEST_CONTAINER=my-test-bucket
export GCP_SERVICE_ACCOUNT_KEY_JSON='{"type":"service_account","project_id":"...","private_key":"..."}'

mvn -pl cloud-storage-sdk-gcp test
```

---

### Oracle Cloud Infrastructure Object Storage

**Test class:** `cloud-storage-sdk-oci` → `OciStorageServiceTest`

| Variable | Required | Description |
|---|---|---|
| `OCI_STORAGE_TEST_ENABLED` | Yes | Any non-empty value enables the tests |
| `OCI_STORAGE_TEST_CONTAINER` | Yes | Name of a pre-existing, writable OCI bucket |
| `OCI_STORAGE_TEST_AUTH_TYPE` | No | `ACCESS_KEY` (default), `INSTANCE_PROFILE`, or `IAM` |
| `OCI_USER_ID` | If ACCESS_KEY | User OCID (mapped to `storageKey`) |
| `OCI_PRIVATE_KEY_PEM` | If ACCESS_KEY | PEM content of the API signing key (mapped to `storageSecret`) |
| `OCI_TENANCY_ID` | If ACCESS_KEY | Tenancy OCID |
| `OCI_FINGERPRINT` | If ACCESS_KEY | Key fingerprint |
| `OCI_REGION` | No | OCI region (default: `us-ashburn-1`) |

#### Example: OCI Compute instance with Instance Principal

On an OCI Compute instance or OKE pod with an Instance Principal dynamic group
policy granting `manage objects` and `manage buckets`:

```bash
export OCI_STORAGE_TEST_ENABLED=true
export OCI_STORAGE_TEST_CONTAINER=my-test-bucket
export OCI_STORAGE_TEST_AUTH_TYPE=INSTANCE_PROFILE
export OCI_REGION=us-ashburn-1

mvn -pl cloud-storage-sdk-oci test
```

#### Example: Local development with API key

```bash
export OCI_STORAGE_TEST_ENABLED=true
export OCI_STORAGE_TEST_CONTAINER=my-test-bucket
export OCI_USER_ID=ocid1.user.oc1..aaaa...
export OCI_TENANCY_ID=ocid1.tenancy.oc1..aaaa...
export OCI_FINGERPRINT=aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99
export OCI_PRIVATE_KEY_PEM="$(cat ~/.oci/oci_api_key.pem)"
export OCI_REGION=us-ashburn-1

mvn -pl cloud-storage-sdk-oci test
```

---

## Running All Integration Tests

To run integration tests for multiple (or all) providers at once, set the
relevant environment variables and run:

```bash
mvn test
```

Only the CSPs whose `*_STORAGE_TEST_ENABLED` variable is set will actually
execute; all others are skipped.

## Running a Single CSP Module

```bash
# AWS only
mvn -pl cloud-storage-sdk-aws test

# Azure only
mvn -pl cloud-storage-sdk-azure test

# GCP only
mvn -pl cloud-storage-sdk-gcp test

# OCI only
mvn -pl cloud-storage-sdk-oci test
```

## Running a Specific Test Method

```bash
mvn -pl cloud-storage-sdk-aws test \
    -Dtest="AwsStorageServiceTest#testUploadAndDownload"
```

---

## CI/CD Notes

- **Default behaviour:** `mvn test` runs unit tests and skips all integration
  tests (since `*_STORAGE_TEST_ENABLED` is not set). This is safe for all CI
  environments.
- **Integration gate:** To add an integration test gate in CI, set the required
  environment variables as secrets in your CI system and enable the specific CSP
  tests in a dedicated pipeline stage.
- **Cleanup:** Integration tests create objects under a unique UUID-prefixed
  path and delete them in `@AfterAll`. If a test run is interrupted, you may
  need to manually clean up objects matching the `test-<uuid>/` prefix from the
  test bucket.

## Prerequisites

| Provider | Minimum permissions |
|---|---|
| AWS | `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` on the test bucket |
| Azure | `Storage Blob Data Contributor` role on the test container |
| GCP | `roles/storage.objectAdmin` on the test bucket |
| OCI | `manage objects` and `read buckets` in the test compartment |
