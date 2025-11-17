//! Lightweight helpers around `aws_sdk_s3` tailored for the book's ingestion tasks.
//!
//! The module focuses on two scenarios: spinning up disposable buckets and streaming
//! parquet artefacts without loading them entirely into memory.

use anyhow::Result;
use aws_sdk_s3::{
    error::SdkError, operation::head_object::HeadObjectError, primitives::ByteStream,
};
use tokio::io::AsyncReadExt;
use tracing::debug;

/// Minimal façade for the S3 operations required by the data workflows.
#[derive(Clone, Debug)]
pub(crate) struct S3Client {
    client: aws_sdk_s3::Client,
    region: String,
    bucket_name: String,
}

impl S3Client {
    /// Constructs a client bound to the given bucket.
    pub(crate) fn new(config: S3Config, bucket_name: String) -> Self {
        let region = config.region.clone();
        let config = config.get_s3_config();
        let client = aws_sdk_s3::Client::from_conf(config.clone());
        Self {
            client,
            region,
            bucket_name,
        }
    }

    /// Returns a clone of the configured bucket name.
    pub(crate) fn get_bucket_name(&self) -> String {
        self.bucket_name.clone()
    }

    /// Returns true if the configured bucket exists for the active credentials.
    pub(crate) async fn bucket_exists(&self) -> Result<bool> {
        let bucket_list = self.client.list_buckets().send().await?;

        let bucket_found = bucket_list
            .buckets()
            .iter()
            .filter_map(|b| b.name())
            .any(|name| name == self.bucket_name);

        Ok(bucket_found)
    }

    /// Creates the configured bucket, honouring the region constraint.
    pub(crate) async fn create_bucket(&self) -> Result<()> {
        let constraint = aws_sdk_s3::types::BucketLocationConstraint::from(self.region.as_str());
        let cfg = aws_sdk_s3::types::CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();
        self.client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(self.bucket_name.as_str())
            .send()
            .await?;

        Ok(())
    }

    /// Returns all object keys in the bucket, in lexicographic order.
    pub(crate) async fn list_objects(&self) -> Result<Vec<String>> {
        let aws_object_list = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let object_list = aws_object_list
            .contents()
            .iter()
            .filter_map(|o| o.key())
            .map(String::from)
            .collect();
        Ok(object_list)
    }

    /// Deletes every object then removes the bucket (intended for ephemeral datasets).
    pub(crate) async fn delete_bucket(&self) -> Result<()> {
        debug!("Deleting bucket {}.", &self.bucket_name);
        let objects_to_delete = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .send()
            .await?;
        for object in objects_to_delete.contents() {
            if let Some(key) = object.key() {
                self.client
                    .delete_object()
                    .bucket(&self.bucket_name)
                    .key(key)
                    .send()
                    .await?;
            }
        }
        self.client
            .delete_bucket()
            .bucket(&self.bucket_name)
            .send()
            .await?;
        debug!("Bucket deleted.");
        Ok(())
    }

    /// Streams a parquet file to S3 using multipart upload to cap memory usage.
    pub(crate) async fn stream_chunked_parquet_to_s3(
        &self,
        file_path: &str,
        key: &str,
    ) -> Result<()> {
        let s3_path = format!("s3://{}/{}", self.bucket_name, key);
        debug!("Streaming to S3: {}", s3_path);

        let upload_id = self.start_multipart_upload(key).await?;
        let completed_parts = self.upload_file_parts(file_path, key, &upload_id).await?;
        self.complete_multipart_upload(key, &upload_id, completed_parts)
            .await?;
        Ok(())
    }

    /// Starts a multipart upload and returns the upload ID required for subsequent parts.
    async fn start_multipart_upload(&self, key: &str) -> Result<String> {
        let multipart = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await?;

        multipart
            .upload_id()
            .ok_or_else(|| anyhow::anyhow!("No upload ID returned."))
            .map(|s| s.to_string())
    }

    /// Uploads a local file in 5 MiB parts and returns the metadata needed to finalise the upload.
    async fn upload_file_parts(
        &self,
        file_path: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<aws_sdk_s3::types::CompletedPart>> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut part_number = 1;
        let mut completed_parts = Vec::new();
        let chunk_size = 5 * 1024 * 1024;

        loop {
            let mut buffer = vec![0u8; chunk_size];
            let mut bytes_read = 0;

            while bytes_read < chunk_size {
                match file.read(&mut buffer[bytes_read..]).await? {
                    0 => break, // EOF
                    n => bytes_read += n,
                }
            }

            if bytes_read == 0 {
                break;
            }

            // By trimming the buffer we keep heap usage bounded and satisfy the minimum
            // part size for S3 (except for the final, potentially smaller, chunk).
            buffer.truncate(bytes_read);

            debug!("Uploading part {} with {} bytes", part_number, bytes_read);

            let upload_part = self
                .client
                .upload_part()
                .bucket(&self.bucket_name)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(buffer))
                .send()
                .await?;

            let e_tag = upload_part
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("No ETag in upload response."))?;

            completed_parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .e_tag(e_tag)
                    .part_number(part_number)
                    .build(),
            );

            part_number += 1;
        }

        Ok(completed_parts)
    }

    /// Finalises the multipart upload using the collected part metadata.
    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        completed_parts: Vec<aws_sdk_s3::types::CompletedPart>,
    ) -> Result<()> {
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }

    /// Returns true if the given key exists, treating 404 as absence to avoid confusing network
    /// errors with a missing object.
    pub(crate) async fn object_exists(&self, key: &str) -> Result<bool> {
        let response = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await;

        match response {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(err))
                if matches!(err.err(), HeadObjectError::NotFound(_)) =>
            {
                Ok(false)
            }
            Err(err) => Err(err.into()),
        }
    }
}

/// Configuration required to connect to an S3-compatible endpoint.
pub(crate) struct S3Config {
    pub(crate) region: String,
    pub(crate) url: String,
    pub(crate) username: String,
    pub(crate) password: String,
}

impl S3Config {
    /// Builds static credentials from the username/password pair.
    fn get_credentials(&self) -> aws_sdk_s3::config::Credentials {
        aws_sdk_s3::config::Credentials::new(
            self.username.clone(),
            self.password.clone(),
            None,
            None,
            "loaded_from_code",
        )
    }

    /// Assembles an SDK config pointing at the desired endpoint and region.
    fn get_s3_config(&self) -> aws_sdk_s3::Config {
        let creds = self.get_credentials();
        aws_sdk_s3::config::Builder::new()
            .endpoint_url(self.url.clone())
            .credentials_provider(creds)
            .region(aws_sdk_s3::config::Region::new(self.region.clone()))
            .build()
    }
}
