use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::Serialize;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub(crate) fn main() -> Result<()> {
    let state = Arc::new(Mutex::new(State::default()));
    Runtime::init(try_main(state))
}

async fn try_main(state: Arc<Mutex<State>>) -> Result<()> {
    let handler = Arc::new(Handler::new(state));
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct State {
    seen: HashSet<i64>,
}

impl State {
    fn add(&mut self, v: i64) {
        self.seen.insert(v);
    }
    fn all(&self) -> HashSet<i64> {
        self.seen.clone()
    }
}

#[derive(Clone)]
struct Handler {
    state: Arc<Mutex<State>>,
}

impl Handler {
    pub fn new(state: Arc<Mutex<State>>) -> Self {
        Handler { state }
    }

    fn to_response(&self, request: BroadcastRequest) -> std::io::Result<BroadcastResponse> {
        match request {
            BroadcastRequest::Broadcast(v) => {
                self.state.lock().unwrap().add(v);
                Ok(BroadcastResponse::BroadcastOk {})
            }
            BroadcastRequest::Read() => Ok(BroadcastResponse::ReadOk {
                messages: self.state.lock().unwrap().all(),
            }),
            BroadcastRequest::Topology(_) => Ok(BroadcastResponse::TopologyOk {}),
        }
    }

    fn to_request(&self, message: &Message) -> std::io::Result<BroadcastRequest> {
        match message.get_type() {
            "broadcast" => {
                let value = message
                    .body
                    .extra
                    .get("message")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "message not found for broadcast",
                        )
                    });
                Ok(BroadcastRequest::Broadcast(value?))
            }
            "read" => Ok(BroadcastRequest::Read()),
            "topology" => Ok(BroadcastRequest::Topology(Neighbours::default())),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format! {"Did not recognise input: {}", message.get_type()},
            )),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if let Ok(request) = self.to_request(&req) {
            if let Ok(response) = self.to_response(request) {
                return runtime.reply(req, response).await;
            }
        }

        done(runtime, req)
    }
}

#[derive(Clone, Debug)]
enum BroadcastRequest {
    Broadcast(i64),
    Read(),
    Topology(Neighbours),
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BroadcastResponse {
    BroadcastOk {},
    ReadOk { messages: HashSet<i64> },
    TopologyOk {},
}

#[derive(Default, Clone, Debug)]
struct Neighbours {}
