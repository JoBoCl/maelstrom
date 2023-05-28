#![feature(async_closure)]
use async_trait::async_trait;
use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde_json::json;

use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};
use tokio_context::context::Context;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let block = Arc::new(Mutex::new(Block::default()));
    let handler = Arc::new(Handler::new(runtime.clone(), block));
    runtime.with_handler(handler).run().await
}

static LIMIT: u64 = 20;

#[derive(Clone)]
struct Handler {
    storage: Storage,
    block: Arc<Mutex<Block>>,
}

#[derive(Clone, Default)]
struct Block {
    internal_counter: u64,
    block_start: u64,
}

impl Block {
    fn next_block(&self) -> bool {
        self.internal_counter == LIMIT
    }

    fn next_id(&mut self) -> u64 {
        let val = self.internal_counter + self.block_start;
        self.internal_counter += 1;
        val
    }

    fn update_block(&mut self, block: u64) {
        self.block_start = block;
        self.internal_counter = 0;
    }
}

impl Handler {
    pub fn new(runtime: Runtime, block: Arc<Mutex<Block>>) -> Self {
        Handler {
            block,
            storage: lin_kv(runtime),
        }
    }
    async fn cas(&self, old: u64, new: u64) -> bool {
        let (ctx, _handler) = Context::new();
        self.storage
            .cas(ctx, "x".to_string(), old, new, old == 0)
            .await
            .is_ok()
    }
    async fn read(&self) -> u64 {
        let (ctx, _handler) = Context::new();
        self.storage.get(ctx, "x".to_string()).await.unwrap_or(0)
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "generate" {
            if self.block.lock().unwrap().next_block() {
                let mut val = self.read().await;
                while !self.cas(val, val + LIMIT).await {
                    val = self.read().await
                }
                self.block.lock().unwrap().update_block(val);
            }
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
