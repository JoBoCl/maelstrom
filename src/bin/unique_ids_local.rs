use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde_json::json;
use std::sync::{Arc, Mutex};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let block = Arc::new(Mutex::new(Block::default()));
    let handler = Arc::new(Handler::new(block));
    runtime.with_handler(handler).run().await
}

#[derive(Clone)]
struct Handler {
    block: Arc<Mutex<Block>>,
}

#[derive(Clone, Default)]
struct Block {
    internal_counter: usize,
    offset: usize,
    network_size: usize,
}

impl Block {
    fn next_id(&mut self) -> usize {
        let val = self.internal_counter * self.network_size + self.offset;
        self.internal_counter += 1;
        val
    }
}

impl Handler {
    pub fn new(block: Arc<Mutex<Block>>) -> Self {
        Handler { block }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "init" {
            let node_id = req.body.extra.get("node_id").unwrap();
            let nodes = req.body.extra.get("node_ids").unwrap().as_array().unwrap();
            self.block.lock().unwrap().network_size = nodes.len();

            self.block.lock().unwrap().offset = nodes.iter().position(|v| v == node_id).unwrap();
        }
        if req.get_type() == "generate" {
            let mut response = req.body.clone().with_type("generate_ok");
            response.extra.insert(
                "id".to_string(),
                json!(self.block.lock().unwrap().next_id()),
            );
            return runtime.reply(req, response).await;
        }

        done(runtime, req)
    }
}
