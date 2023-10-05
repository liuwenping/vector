use std::collections::HashMap;
use crate::sinks::prelude::*;
use vector_core::{
    internal_event::{
        ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle as _, Output, Protocol,
    },
    EstimatedJsonEncodedSizeOf,
};
use crate::sinks::bkgse::client::GseClient;

pub struct BkGseSink {
    client: GseClient,
    data_id: u32,
    gse_index: u32,

    batch_settings: BatcherSettings,
}

#[async_trait::async_trait]
impl StreamSink<Event> for BkGseSink {
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}


impl BkGseSink {
    pub(super) fn new(
        client: GseClient,
        data_id: u32,
        batch_settings: BatcherSettings,
    ) -> crate::Result<Self> {
        Ok(BkGseSink{
            client,
            data_id,
            gse_index: 0,

            batch_settings,
        })
    }

    async fn run_inner(mut self, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let bytes_sent = register!(BytesSent::from(Protocol("bkgse".into(),)));
        let events_sent = register!(EventsSent::from(Output(None)));
        // let batch_input =  input.batched(self.batch_settings);
        let mut events_buff: Vec<Event> = Vec::with_capacity(self.batch_settings.item_limit + 1);

        while let Some(mut event) = input.next().await {
            if events_buff.len() >= self.batch_settings.item_limit {
                let mut items: Vec<HashMap<String, Value>> = Vec::with_capacity(events_buff.len()) ;
                for i in 0..events_buff.len() {
                    let log_event = events_buff.get(i).unwrap().as_log();
                    let mut item: HashMap<String, Value> = HashMap::new();
                    let data = log_event.get_message().unwrap();
                    item.insert(String::from("data"), data.clone());
                    item.insert(String::from("iterationindex"), Value::Integer(i as i64));

                    items.push(item);
                }

                self.gse_index += 1;
                let utc_now = chrono::Utc::now();
                let local_now = chrono::Local::now();
                let json_data = serde_json::json!({
                    "bizid":0,
                    "bk_agent_id":"020000000052540084142a16942509910802",
                    "bk_biz_id":0,
                    "bk_host_id":0,
                    "cloudid":0,
                    "dataid": self.data_id,
                    "datetime": local_now.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "utctime": utc_now.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "filename": event.as_log().get("file"),
                    "gseindex": self.gse_index,
                    "time": utc_now.timestamp(),
                    "items": items,
                });
                let data = serde_json::to_string(&json_data).unwrap();
                self.client.send_event(data.as_bytes().to_vec(), self.data_id);

                bytes_sent.emit(ByteSize(data.len()));
                let event_byte_size = event.estimated_json_encoded_size_of();

                events_sent.emit(CountByteSize(1, event_byte_size));

                let finalizers = event.take_finalizers();
                finalizers.update_status(EventStatus::Delivered);

                events_buff.clear();
            } else {
                events_buff.push(event);
            }
        }

        Ok(())
    }
}