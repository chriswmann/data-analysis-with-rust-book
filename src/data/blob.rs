use anyhow::Result;

pub(crate) struct S3Client {
    client: aws_sdk_s3::Client,
    config: aws_sdk_s3::Config,
    bucket_name: String,
}

impl S3Client {
    pub(crate) fn new(config: aws_sdk_s3::Config, bucket_name: String) -> Self {
        let client = aws_sdk_s3::Client::from_conf(config.clone());
        Self {
            client,
            config,
            bucket_name,
        }
    }

    pub(crate) async fn bucket_exists(&self) -> Result<bool> {
        let bucket_list = self.client.list_buckets().send().await?;

        let bucket_found = bucket_list
            .buckets()
            .iter()
            .filter_map(|b| b.name())
            .any(|name| name == self.bucket_name);

        Ok(bucket_found)
    }

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

    pub(crate) async fn delete_bucket(&self) -> Result<()> {
        let objects_to_delete = self.client.list_objects_v2().send().await?;
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
        self.client.delete_bucket().send().await?;
        Ok(())
    }
}

pub(crate) struct S3Config {
    region: String,
    bucket_name: String,
    url: String,
    username: String,
    password: String,
}

impl S3Config {
    fn get_credentials(&self) -> aws_sdk_s3::config::Credentials {
        aws_sdk_s3::config::Credentials::new(
            self.username.clone(),
            self.password.clone(),
            None,
            None,
            "loaded_from_code",
        )
    }

    fn get_config(&self) -> aws_sdk_s3::Config {
        let creds = self.get_credentials();
        aws_sdk_s3::config::Builder::new()
            .endpoint_url(self.url.clone())
            .credentials_provider(creds)
            .region(aws_sdk_s3::config::Region::new(self.region.clone()))
            .build()
    }
}
