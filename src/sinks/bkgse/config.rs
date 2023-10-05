use crate::sinks::bkgse::client::GseClient;
use crate::sinks::prelude::*;
use crate::sinks::util::RealtimeSizeBasedDefaultBatchSettings;
use super::sink::BkGseSink;

#[configurable_component(sink("bkgse"))]
#[derive(Clone, Debug)]
/// A socket sink that dumps its output to blueking gse agent.
pub struct BkGseSinkConfig {

    #[configurable(derived)]
    pub(super) encoding: EncodingConfig,

    /// agent socket file
    #[configurable(metadata(docs::examples = "/var/run/ipc.state.report"))]
    pub(super) endpoint: String,

    /// gse data id
    #[configurable(metadata(docs::examples = 1001))]
    pub(super) data_id: u32,

    #[configurable(derived)]
    #[serde(default)]
    pub(super) batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for BkGseSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"endpoint = "/var/run/ipc.state.report""#,
        ).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "bkgse")]
impl SinkConfig for BkGseSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let batch_settings = self.batch.validate()?.into_batcher_settings()?;
        let client = self.build_gse_client(&cx)?;

        let healthcheck = Box::pin(async move { Ok(()) });
        // let healthcheck = Box::pin(healthcheck(client));
        let sink = BkGseSink::new(client, self.data_id, batch_settings)?;
        let sink = VectorSink::from_event_streamsink(sink);

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}


impl BkGseSinkConfig {
    #[allow(dead_code)]
    fn build_gse_client(&self, _cx: &SinkContext) -> crate::Result<GseClient> {
        Ok(GseClient::new(self.endpoint.clone()))
    }
}

#[allow(dead_code)]
async fn healthcheck(mut client: GseClient) -> crate::Result<()> {
    client.check();
    Ok(())
}