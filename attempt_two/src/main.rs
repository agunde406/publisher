// Copyright 2018-2022 Cargill Incorporated
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
mod one_batch;

use std::marker::PhantomData;
use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::thread;

enum PublishMessage {
    Cancel,
    Finish,
    Dropped,
}

// ~~~~~~~~~~~~~  Publisher Struct Definitions ~~~~~~~~~~~~~~~~~~~~~~~~~
struct PublishStarter<
    B: 'static + Batch,
    C: 'static + PublisherContext,
    R: 'static + PublishedResult,
> {
    result_creator_factory: Box<dyn PublishedResultCreatorFactory<B, C, R>>,
    batch_verifier_factory: Box<dyn BatchVerifierFactory<B, C>>,
}

impl<B: 'static + Batch, C: 'static + PublisherContext, R: 'static + PublishedResult>
    PublishStarter<B, C, R>
{
    pub fn new(
        result_creator_factory: Box<dyn PublishedResultCreatorFactory<B, C, R>>,
        batch_verifier_factory: Box<dyn BatchVerifierFactory<B, C>>,
    ) -> Self {
        Self {
            result_creator_factory,
            batch_verifier_factory,
        }
    }
}

impl<B: Batch, C: PublisherContext, R: PublishedResult> PublishStarter<B, C, R> {
    /// Start building the next publishable unit, referred to as a block going forward
    /// The publisher will start pulling batches off of a pending queue for the provided service
    /// and
    ///
    /// # Arguments
    ///
    /// * `context` - Implementation specific context for the publisher
    /// * `batches` - An interator the returns the next batch to execute
    ///
    /// Returns a PublishFinisher that can be used to finish or cancel the executing batch
    ///
    fn start(
        &mut self,
        mut context: C,
        mut batches: Box<dyn PendingBatches<B>>,
    ) -> Result<PublishFinisher<B, C, R>, InternalError> {
        let (sender, rc) = channel();
        let mut verifier = self.batch_verifier_factory.start(context.clone())?;
        let result_creator = self.result_creator_factory.new_creator()?;
        let join_handle = thread::spawn(move || loop {
            // TODO fix very tight loop here
            // Should the batch verifier take the iterator here instead of a having a batch passed
            //  one by one?
            if let Some(batch) = batches.next()? {
                verifier.add_batch(batch)?;
            }

            // Check to see if the batch result should be finished/canceled
            match rc.try_recv() {
                Ok(PublishMessage::Cancel) => {
                    println!("Received Cancel");

                    verifier.cancel()?;

                    return Ok(None);
                }
                Ok(PublishMessage::Finish) => {
                    println!("Received Finish");

                    let results = verifier.finalize()?;

                    let mut txn_receipts = Vec::new();
                    for batch_result in results.iter() {
                        let id = batch_result.batch.id();
                        context.add_batch_result(id, batch_result.receipts.to_vec());

                        txn_receipts.append(&mut batch_result.receipts.to_vec())
                    }

                    let state_root = context.compute_state_id(&txn_receipts)?;

                    return Ok(Some(result_creator.create(context, results, state_root)?));
                }
                Ok(PublishMessage::Dropped) => {
                    println!("Finisher was dropped, so break loop");
                    return Ok(None);
                }
                Err(TryRecvError::Empty) => (),
                Err(_) => {
                    println!("Disconnected");
                    return Err(InternalError);
                }
            };
        });

        Ok(PublishFinisher::new(sender, join_handle))
    }
}

struct PublishFinisher<B: Batch, C: PublisherContext, R: PublishedResult> {
    sender: Option<Sender<PublishMessage>>,
    join_handle: Option<thread::JoinHandle<Result<Option<R>, InternalError>>>,
    _context: PhantomData<C>,
    _batch: PhantomData<B>,
}

impl<B: Batch, C: PublisherContext, R: PublishedResult> PublishFinisher<B, C, R> {
    pub fn new(
        sender: Sender<PublishMessage>,
        join_handle: thread::JoinHandle<Result<Option<R>, InternalError>>,
    ) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
            _context: PhantomData,
            _batch: PhantomData,
        }
    }

    /// Finish constructing the block, returning a result that contains the bytes that consensus
    /// must agree upon and a list of TransactionReceipts. Any batches that are not finished
    /// processing, will be returned to the pending state.
    fn finish(mut self) -> Result<R, InternalError> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(PublishMessage::Finish)
                .map_err(|_| InternalError)?;
            match self
                .join_handle
                .take()
                .ok_or(InternalError)?
                .join()
                .map_err(|_| InternalError)?
            {
                Ok(Some(result)) => Ok(result),
                // no result returned
                Ok(None) => Err(InternalError),
                Err(_) => Err(InternalError),
            }
        } else {
            // already called finish or cancel
            Err(InternalError)
        }
    }

    /// Cancel the currently building block, putting all batches back into a pending state
    fn cancel(mut self) -> Result<(), InternalError> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(PublishMessage::Cancel)
                .map_err(|_| InternalError)?;
            match self
                .join_handle
                .take()
                .ok_or(InternalError)?
                .join()
                .map_err(|_| InternalError)?
            {
                // Did not expect any results
                Ok(Some(_)) => Err(InternalError),
                Ok(None) => Ok(()),
                Err(_) => Err(InternalError),
            }
        } else {
            // already called finish or cancel
            Err(InternalError)
        }
    }
}

impl<B: Batch, C: PublisherContext, R: PublishedResult> Drop for PublishFinisher<B, C, R> {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            match sender.send(PublishMessage::Dropped) {
                Ok(_) => (),
                Err(_) => {
                    println!("Unable to shutdown Publisher thread")
                }
            }
        }
    }
}

/// This trait would go in sawtooth-lib
pub trait PublishedResult: Send {}

pub trait PublishedResultCreatorFactory<B: Batch, C: PublisherContext, R: PublishedResult> {
    fn new_creator(&self) -> Result<Box<dyn PublishedResultCreator<B, C, R>>, InternalError>;
}

pub trait PublishedResultCreator<B: Batch, C: PublisherContext, R: PublishedResult>: Send {
    fn create(
        &self,
        context: C,
        batch_results: Vec<BatchExecutionResult<B>>,
        resulting_state_root: String,
    ) -> Result<R, InternalError>;
}

/// This trait would go in sawtooth-lib
pub trait PublisherContext: Clone + Send {
    fn add_batch_result(&mut self, batch_id: String, receipts: Vec<TransactionReceipt>);

    fn compute_state_id(
        &mut self,
        txn_receipts: &[TransactionReceipt],
    ) -> Result<String, InternalError>;
}

// This trait would go in sawtooth-lib
pub trait Batch: Send {
    fn id(&self) -> String;
}

pub trait BatchVerifierFactory<B: Batch, C: PublisherContext> {
    fn start(&mut self, context: C) -> Result<Box<dyn BatchVerifier<B, C>>, InternalError>;
}

pub trait BatchVerifier<B: Batch, C: PublisherContext>: Send {
    fn add_batch(&mut self, batch: B) -> Result<(), InternalError>;

    fn finalize(&mut self) -> Result<Vec<BatchExecutionResult<B>>, InternalError>;

    fn cancel(&mut self) -> Result<(), InternalError>;
}

pub trait PendingBatches<B: Batch>: Send {
    fn next(&mut self) -> Result<Option<B>, InternalError>;
}

/// This struct could go into sawtooth-lib
#[derive(Clone, Debug)]
pub struct InternalError;

/// This struct is in sawtooth-lib
#[derive(Debug, Clone)]
pub struct TransactionReceipt;

/// Result of executing a batch.
#[derive(Debug, Clone)]
pub struct BatchExecutionResult<B: Batch> {
    /// The `BatchPair` which was executed.
    pub batch: B,

    /// The receipts for each transaction in the batch.
    pub receipts: Vec<TransactionReceipt>,
}

fn main() -> Result<(), InternalError> {
    use one_batch::{
        BatchContext, BatchIter, OneBatch, OneBatchVerifierFactory, PublishBatchResult,
        PublishBatchResultCreatorFactory,
    };

    let result_creator_factory = Box::new(PublishBatchResultCreatorFactory::new());
    let batch_verifier_factory = Box::new(OneBatchVerifierFactory::new());
    let mut publisher_starter: PublishStarter<OneBatch, BatchContext, PublishBatchResult> =
        PublishStarter::new(result_creator_factory, batch_verifier_factory);

    let pending_batches = Box::new(BatchIter::new());

    let context = BatchContext::new(
        "test_circuit".to_string(),
        "test_service".to_string(),
        "abcd".to_string(),
    );

    let publisher_finisher = publisher_starter.start(context, pending_batches)?;
    publisher_finisher.cancel()?;

    let pending_batches = Box::new(BatchIter::new());

    let context = BatchContext::new(
        "test_circuit".to_string(),
        "test_service".to_string(),
        "abcd".to_string(),
    );

    let publisher_finisher = publisher_starter.start(context, pending_batches)?;
    let result = publisher_finisher.finish()?;

    println!("Results {:?}", result);

    let pending_batches = Box::new(BatchIter::new());

    let context = BatchContext::new(
        "test_circuit".to_string(),
        "test_service".to_string(),
        "abcd".to_string(),
    );

    let publisher_finisher = publisher_starter.start(context, pending_batches)?;
    drop(publisher_finisher);
    Ok(())
}
