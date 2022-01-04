// Copyright 2018-2021 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

// ~~~~~~~~~~~~~  Trait Definations ~~~~~~~~~~~~~~~~~~~~~~~~~

/// This trait would go in sawtooth-lib
pub trait Publisher<C: PublisherContext, R: PublishedResult> {
    /// Start building the next publishable unit, referred to as a block going forward
    /// The publisher will start pulling batches off of a pending queue for the provided service
    /// and
    ///
    /// # Arguments
    ///
    /// * `context` - Implementation specific context for the publisher
    fn start(&mut self, context: C) -> Result<(), InternalError>;

    /// Finish constructing the block, returning a result that contains the bytes that consensus
    /// must agree upon and a list of TransactionReceipts. Any batches that are not finished
    /// processing, will be returned to the pending state.
    ///
    /// # Arguments
    ///
    /// * `context` - Implementation specific context for the publisher
    fn finish(&mut self, context: C) -> Result<R, InternalError>;

    /// Cancel the currently building block, putting all batches back into a pending state
    ///
    /// # Arguments
    ///
    /// * `context` - Implementation specific context for the publisher
    fn cancel(&mut self, context: C) -> Result<(), InternalError>;
}

/// This trait would go in sawtooth-lib
pub trait PublishedResult {
    /// Get the bytes that need to be agreed on
    fn get_bytes(&self) -> Result<Vec<u8>, InternalError>;
}

/// This trait would go in sawtooth-lib
pub trait PublisherContext {}

// ~~~~~~~~~~~~~  Example Trait Implementations ~~~~~~~~~~~~~~~~~~~~~~~~~

/// This implementation could go into Scabbard
pub struct BatchPublisher;

/// This implementation could go into Scabbard
impl Publisher<BatchContext, PublishBatchResult> for BatchPublisher {
    fn start(&mut self, _context: BatchContext) -> Result<(), InternalError> {
        Ok(())
    }

    fn finish(&mut self, _context: BatchContext) -> Result<PublishBatchResult, InternalError> {
        Ok(PublishBatchResult {
            state_root_hash: "test".to_string(),
            receipts: vec![TransactionReceipt],
        })
    }

    fn cancel(&mut self, _context: BatchContext) -> Result<(), InternalError> {
        Ok(())
    }
}

/// This implementation could go into Scabbard
#[derive(Clone, Debug)]
pub struct PublishBatchResult {
    state_root_hash: String,
    receipts: Vec<TransactionReceipt>,
}

/// This implementation could go into Scabbard
impl PublishedResult for PublishBatchResult {
    fn get_bytes(&self) -> Result<Vec<u8>, InternalError> {
        parse_hex(&self.state_root_hash)
    }
}

/// This implementation could go into Scabbard
#[derive(Clone, Debug)]
pub struct BatchContext {
    circuit_id: String,
    service_id: String,
    starting_commit_hash: String,
}

/// This implementation could go into Scabbard
impl PublisherContext for BatchContext {}

/// This implementation could go into Sawtooth
pub struct BlockPublisher;

/// This implementation could go into Sawtooth
impl Publisher<BlockContext, PublishBlockResult> for BlockPublisher {
    fn start(&mut self, _context: BlockContext) -> Result<(), InternalError> {
        Ok(())
    }

    fn finish(&mut self, _context: BlockContext) -> Result<PublishBlockResult, InternalError> {
        Ok(PublishBlockResult {
            block_id: "test".to_string(),
            _receipts: vec![TransactionReceipt],
        })
    }

    fn cancel(&mut self, _context: BlockContext) -> Result<(), InternalError> {
        Ok(())
    }
}

/// This implementation could go into Sawtooth
#[derive(Clone)]
pub struct PublishBlockResult {
    block_id: String,
    _receipts: Vec<TransactionReceipt>,
}

/// This implementation could go into Sawtooth
#[derive(Clone)]
pub struct BlockContext {
    _starting_commit_hash: String,
    _previous_block_id: String,
}

/// This implementation could go into Sawtooth
impl PublisherContext for BlockContext {}

/// This implementation could go into Sawtooth
impl PublishedResult for PublishBlockResult {
    fn get_bytes(&self) -> Result<Vec<u8>, InternalError> {
        parse_hex(&self.block_id)
    }
}

/// This struct could go into sawtooth-lib
#[derive(Clone, Debug)]
pub struct InternalError;

/// This struct is in sawtooth-lib
#[derive(Clone, Debug)]
pub struct TransactionReceipt;

pub fn parse_hex(hex: &str) -> Result<Vec<u8>, InternalError> {
    if hex.len() % 2 != 0 {
        return Err(InternalError);
    }

    let mut res = vec![];
    for i in (0..hex.len()).step_by(2) {
        res.push(u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| InternalError)?);
    }

    Ok(res)
}

// ~~~~~~~~~~~~~ Example Implementations of a Manager ~~~~~~~~~~~~~~~~~~

/// Messages that will be sent to the Manager
enum PublishMessage<C: PublisherContext, R: PublishedResult> {
    Start {
        context: C,
        sender: Sender<Result<(), InternalError>>,
    },
    Finish {
        context: C,
        sender: Sender<Result<R, InternalError>>,
    },
    Cancel {
        context: C,
        sender: Sender<Result<(), InternalError>>,
    },
    Shutdown,
}

/// The struct that will implement Publisher and be passed to the Coordinator/Consensus
#[derive(Clone)]
struct PublishHandle<C: PublisherContext, R: PublishedResult> {
    sender: Sender<PublishMessage<C, R>>,
}

impl<C: PublisherContext, R: PublishedResult> PublishHandle<C, R> {
    fn new(sender: Sender<PublishMessage<C, R>>) -> Self {
        PublishHandle { sender }
    }
}

impl<C: PublisherContext, R: PublishedResult> Publisher<C, R> for PublishHandle<C, R> {
    fn start(&mut self, context: C) -> Result<(), InternalError> {
        let (sender, rc) = channel();
        let msg = PublishMessage::Start { context, sender };
        self.sender.send(msg).unwrap();
        rc.recv().unwrap()
    }

    fn finish(&mut self, context: C) -> Result<R, InternalError> {
        let (sender, rc) = channel();
        let msg = PublishMessage::Finish { context, sender };
        self.sender.send(msg).unwrap();
        rc.recv().unwrap()
    }

    fn cancel(&mut self, context: C) -> Result<(), InternalError> {
        let (sender, rc) = channel();
        let msg = PublishMessage::Cancel { context, sender };
        self.sender.send(msg).unwrap();
        rc.recv().unwrap()
    }
}

/// The struct that will actually do the publisher work, e.i keeps track of running block/batches
/// and/or passes the work to the BatchVerifier
struct PublishManager {
    incoming_rc: Option<Receiver<PublishMessage<BatchContext, PublishBatchResult>>>,
    publish_handle: PublishHandle<BatchContext, PublishBatchResult>,
    shutdown_handle: Sender<PublishMessage<BatchContext, PublishBatchResult>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl PublishManager {
    pub fn new() -> Self {
        let (sender, receiver) = channel();
        let publish_handle = PublishHandle::new(sender.clone());
        PublishManager {
            incoming_rc: Some(receiver),
            publish_handle,
            shutdown_handle: sender,
            join_handle: None,
        }
    }

    pub fn publish_handle(&self) -> PublishHandle<BatchContext, PublishBatchResult> {
        self.publish_handle.clone()
    }

    pub fn start(&mut self) {
        if let Some(incoming_rc) = self.incoming_rc.take() {
            let join_handle = thread::spawn(move || loop {
                match incoming_rc.recv() {
                    Ok(PublishMessage::Start { context, sender }) => {
                        println!("Received Start: {:?}", context);

                        // Start the execution of a new block/batch

                        // TODO how the interaction with the BatchVerfier and PendingQueue is
                        // implemented is still unknown. That will inform more about how the
                        // threading model may work for the actual creation of the batch/block

                        sender.send(Ok(())).unwrap()
                    }
                    Ok(PublishMessage::Finish { context, sender }) => {
                        println!("Received Finish: {:?}", context);

                        // Finish the validation/creation of block/batch and return the current
                        // completed results

                        sender
                            .send(Ok(PublishBatchResult {
                                state_root_hash: "1234".to_string(),
                                receipts: vec![],
                            }))
                            .unwrap()
                    }
                    Ok(PublishMessage::Cancel { context, sender }) => {
                        println!("Received Cancel: {:?}", context);

                        // Cancel any running work and throw away any pending batch/block results

                        sender.send(Ok(())).unwrap()
                    }
                    Ok(PublishMessage::Shutdown) => {
                        println!("Received Shutdown");
                        break;
                    }
                    Err(err) => {
                        println!("Error {:?}, shutting down", err);
                        break;
                    }
                }
            });

            self.join_handle = Some(join_handle);
        }
    }

    pub fn shutdown(self) {
        self.shutdown_handle.send(PublishMessage::Shutdown).unwrap();
        if let Some(join_handle) = self.join_handle {
            join_handle
                .join()
                .expect("Couldn't cleanly shutdown the PublishManager");
        }
    }
}

fn main() {
    let mut publisher_manager = PublishManager::new();
    publisher_manager.start();

    let mut publisher = publisher_manager.publish_handle();

    let context = BatchContext {
        circuit_id: "test_circuit".to_string(),
        service_id: "test_service".to_string(),
        starting_commit_hash: "abcd".to_string(),
    };

    println!(
        "Create context for Batch for {}:{} with commit hash {}",
        context.circuit_id, context.service_id, context.starting_commit_hash
    );

    publisher.start(context.clone()).unwrap();
    publisher.cancel(context.clone()).unwrap();

    publisher.start(context.clone()).unwrap();
    let batch_result = publisher.finish(context.clone()).unwrap();

    println!(
        "Received publish result with agreed bytes {:?} and txn receipts {:?}",
        batch_result.get_bytes().unwrap(),
        batch_result.receipts,
    );

    publisher_manager.shutdown();
}
