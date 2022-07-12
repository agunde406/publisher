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

use std::sync::mpsc::{channel, Sender};
use std::thread;

enum PublishMessage {
    Cancel,
    Finish,
    Next,
    Dropped,
}

// ~~~~~~~~~~~~~  Publisher Struct Definitions ~~~~~~~~~~~~~~~~~~~~~~~~~
struct PublishFactory<
    B: 'static + Batch,
    C: 'static + PublisherContext,
    R: 'static + Artifact,
    I: 'static + BatchExecutionResult,
> {
    result_creator_factory: Box<
        dyn ArtifactCreatorFactory<
            ArtifactCreator = Box<dyn ArtifactCreator<Context = C, Input = Vec<I>, Artifact = R>>,
        >,
    >,
    batch_verifier_factory:
        Box<dyn BatchVerifierFactory<Batch = B, Context = C, ExecutionResult = I>>,
}

impl<
        B: 'static + Batch,
        C: 'static + PublisherContext,
        R: 'static + Artifact,
        I: 'static + BatchExecutionResult,
    > PublishFactory<B, C, R, I>
{
    pub fn new(
        result_creator_factory: Box<
            dyn ArtifactCreatorFactory<
                ArtifactCreator = Box<
                    dyn ArtifactCreator<Context = C, Input = Vec<I>, Artifact = R>,
                >,
            >,
        >,
        batch_verifier_factory: Box<
            dyn BatchVerifierFactory<Batch = B, Context = C, ExecutionResult = I>,
        >,
    ) -> Self {
        Self {
            result_creator_factory,
            batch_verifier_factory,
        }
    }
}

impl<B: Batch + Clone, C: PublisherContext + Clone, R: Artifact, I: BatchExecutionResult>
    PublishFactory<B, C, R, I>
{
    /// Start building the next publishable unit, referred to as a block going forward
    /// The publisher will start pulling batches off of a pending queue for the provided service
    /// and
    ///
    /// # Arguments
    ///
    /// * `context` - Implementation specific context for the publisher
    /// * `batches` - An interator the returns the next batch to execute
    ///
    /// Returns a PublishHandle that can be used to finish or cancel the executing batch
    ///
    fn start(
        &mut self,
        mut context: C,
        mut batches: Box<dyn PendingBatches<B>>,
    ) -> Result<PublishHandle<R>, InternalError> {
        let (sender, rc) = channel();
        let mut verifier = self.batch_verifier_factory.start(context.clone())?;
        let result_creator = self.result_creator_factory.new_creator()?;
        let join_handle = thread::spawn(move || loop {
            // drain the queue?
            while let Some(batch) = batches.next()? {
                verifier.add_batch(batch)?;
            }

            // Check to see if the batch result should be finished/canceled
            match rc.recv() {
                Ok(PublishMessage::Cancel) => {
                    println!("Received Cancel");

                    verifier.cancel()?;

                    return Ok(None);
                }
                Ok(PublishMessage::Finish) => {
                    println!("Received Finish");

                    let results = verifier.finalize()?;

                    return Ok(Some(result_creator.create(&mut context, results)?));
                }
                Ok(PublishMessage::Dropped) => {
                    println!("Finisher was dropped, so break loop");
                    return Ok(None);
                }
                Ok(PublishMessage::Next) => {
                    while let Some(batch) = batches.next()? {
                        verifier.add_batch(batch)?;
                    }
                }
                Err(_) => {
                    println!("Disconnected");
                    return Err(InternalError);
                }
            };
        });

        Ok(PublishHandle::new(sender, join_handle))
    }
}

struct PublishHandle<R: Artifact> {
    sender: Option<Sender<PublishMessage>>,
    join_handle: Option<thread::JoinHandle<Result<Option<R>, InternalError>>>,
}

impl<R: Artifact> PublishHandle<R> {
    pub fn new(
        sender: Sender<PublishMessage>,
        join_handle: thread::JoinHandle<Result<Option<R>, InternalError>>,
    ) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
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
    pub fn cancel(mut self) -> Result<(), InternalError> {
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

    /// Notify that there is a batch added to the pending queue
    pub fn next_batch(mut self) -> Result<(), InternalError> {
        if let Some(sender) = self.sender.take() {
            sender.send(PublishMessage::Next).map_err(|_| InternalError)
        } else {
            Ok(())
        }
    }
}

impl<R: Artifact> Drop for PublishHandle<R> {
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
trait Artifact: Clone + Send {
    type Identifier: ?Sized;

    fn artifact_id(&self) -> &Self::Identifier;
}

pub trait ArtifactCreatorFactory {
    type ArtifactCreator;

    fn new_creator(&self) -> Result<Self::ArtifactCreator, InternalError>;
}

pub trait ArtifactCreator: Send {
    type Context;
    type Input;
    type Artifact;

    fn create(
        &self,
        context: &mut Self::Context,
        input: Self::Input,
    ) -> Result<Self::Artifact, InternalError>;
}

/// This trait would go in sawtooth-lib
pub trait PublisherContext: Send {}

// This trait would go in sawtooth-lib
pub trait Batch: Send {
    type Transaction;

    fn id(&self) -> &str;

    fn transactions(&self) -> &[Self::Transaction];
}

pub trait Transaction: Send {
    fn id(&self) -> &str;

    fn payload(&self) -> &[u8];

    fn header(&self) -> &[u8];
}

pub trait BatchVerifierFactory {
    type Batch;
    type Context;
    type ExecutionResult: BatchExecutionResult;

    fn start(
        &mut self,
        context: Self::Context,
    ) -> Result<
        Box<
            dyn BatchVerifier<
                Batch = Self::Batch,
                Context = Self::Context,
                ExecutionResult = Self::ExecutionResult,
            >,
        >,
        InternalError,
    >;
}

pub trait BatchVerifier: Send {
    type Batch: Batch;
    type Context;
    type ExecutionResult: BatchExecutionResult;

    fn add_batch(&mut self, batch: Self::Batch) -> Result<(), InternalError>;

    fn finalize(&mut self) -> Result<Vec<Self::ExecutionResult>, InternalError>;

    fn cancel(&mut self) -> Result<(), InternalError>;
}

pub trait PendingBatches<B: Batch>: Send {
    fn next(&mut self) -> Result<Option<B>, InternalError>;
}

/// This struct could go into sawtooth-lib
#[derive(Clone, Debug)]
pub struct InternalError;

/// Result of executing a batch.
pub trait BatchExecutionResult: Send  {}

fn main() -> Result<(), InternalError> {
    use one_batch::{
        BatchContext, BatchIter, OneBatch, OneBatchExecutionResult, OneBatchVerifierFactory,
        PublishBatchResult, PublishBatchResultCreatorFactory,
    };

    let result_creator_factory = Box::new(PublishBatchResultCreatorFactory::new());
    let batch_verifier_factory = Box::new(OneBatchVerifierFactory::new());
    let mut publisher_starter: PublishFactory<
        OneBatch,
        BatchContext,
        PublishBatchResult,
        OneBatchExecutionResult,
    > = PublishFactory::new(result_creator_factory, batch_verifier_factory);

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

    // add sleep to make sure we can see the drop print out
    std::thread::sleep(std::time::Duration::from_secs(1));
    Ok(())
}
