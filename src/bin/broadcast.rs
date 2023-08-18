use async_recursion::async_recursion;
use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio_context::context::Context;

pub(crate) fn main() -> maelstrom::Result<()> {
    color_eyre::install()?;
    let state = Arc::new(Mutex::new(State::default()));
    Runtime::init(try_main(state))
}

async fn try_main(state: Arc<Mutex<State>>) -> maelstrom::Result<()> {
    let handler = Arc::new(Handler::new(state));
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct State {
    seen: HashSet<i64>,
    neighbours: Vec<String>,
}

impl State {
    fn add(&mut self, v: i64) -> bool {
        self.seen.insert(v)
    }
    fn all(&self) -> HashSet<i64> {
        self.seen.clone()
    }
    fn set_neighbours(&mut self, neighbours: Vec<String>) -> color_eyre::Result<()> {
        if self.neighbours.is_empty() {
            self.neighbours = neighbours;
            return Ok(());
        }
        Err(color_eyre::eyre::eyre!("Tried to set neighbours twice"))
    }
    fn neighbours(&self) -> &Vec<String> {
        &self.neighbours
    }
}

#[derive(Clone)]
struct Handler {
    state: Arc<Mutex<State>>,
}

#[async_recursion]
async fn call_with_retry(
    n: String,
    value: i64,
    runtime: Runtime,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (context, _handle) = Context::with_timeout(std::time::Duration::from_millis(100));
    let mut call = match runtime
        .rpc(n.clone(), BroadcastProtocol::Broadcast { message: value })
        .await
    {
        Ok(v) => v,
        Err(e) => {
            return Err(e);
        }
    };
    match call.done_with(context).await {
        Ok(_) => Ok(()),
        Err(e) => {
            log::warn! {"Broadcast {value} to {n} failed with error {e:?}, retrying..."};
            call_with_retry(n, value, runtime).await
        }
    }
}

impl Handler {
    pub fn new(state: Arc<Mutex<State>>) -> Self {
        Handler { state }
    }

    fn to_response(
        &self,
        request: BroadcastProtocol,
        runtime: &Runtime,
    ) -> color_eyre::Result<BroadcastProtocol> {
        match request {
            BroadcastProtocol::Broadcast { message: value } => {
                if self.state.lock().unwrap().add(value) {
                    log::warn! {"New value {value} seen, broadcasting to neighbours."};
                    for n in self.state.lock().unwrap().neighbours() {
                        log::warn! {"Broadcasting {value} to neighbour {n}."};
                        runtime.spawn(call_with_retry(n.clone(), value, runtime.clone()));
                    }
                } else {
                    log::info! {"Already seen {value}, not sending it again."};
                }
                Ok(BroadcastProtocol::BroadcastOk {})
            }
            BroadcastProtocol::Read {} => Ok(BroadcastProtocol::ReadOk {
                messages: self.state.lock().unwrap().all(),
            }),
            BroadcastProtocol::Topology { topology } => {
                if let Some(neighbours) = topology.get(runtime.node_id()) {
                    return match self
                        .state
                        .lock()
                        .unwrap()
                        .set_neighbours(neighbours.clone())
                    {
                        Ok(()) => Ok(BroadcastProtocol::TopologyOk {}),
                        Err(e) => Err(e),
                    };
                }
                Err(
                    color_eyre::eyre::eyre! {"Could not find a list of neighbours for node {}", runtime.node_id()},
                )
            }
            v => Err(color_eyre::eyre::eyre! {"Unexpected input: {v:?}"}),
        }
    }

    fn to_request(&self, message: &Message) -> color_eyre::Result<BroadcastProtocol> {
        match serde_json::from_value(message.body.raw()) {
            Ok(v) => Ok(v),
            Err(e) => Err(color_eyre::eyre::eyre! {e}),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> maelstrom::Result<()> {
        log::error! {"Receved request: {req:?}"};
        if let Ok(request) = self.to_request(&req) {
            if let Ok(response) = self.to_response(request, &runtime) {
                return runtime.reply(req, response).await;
            }
        }

        done(runtime, req)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BroadcastProtocol {
    Broadcast {
        message: i64,
    },
    BroadcastBatch {
        message: Vec<i64>,
    },
    Read {},
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    BroadcastOk {},
    ReadOk {
        messages: HashSet<i64>,
    },
    TopologyOk {},
    BroadcastBatchOk {},
}
