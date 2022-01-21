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

//! Example implmentations, currently return test data but could be updated actually use transact
use super::{
    Batch, BatchExecutionResult, BatchVerifier, BatchVerifierFactory, InternalError,
    PendingBatches, PublishedResult, PublishedResultCreator, PublishedResultCreatorFactory,
    PublisherContext, TransactionReceipt,
};

#[derive(Clone, Debug)]
pub struct PublishBatchResult {
    pub state_root_hash: String,
    pub receipts: Vec<TransactionReceipt>,
}

impl PublishedResult for PublishBatchResult {}

#[derive(Clone, Debug)]
pub struct BatchContext {
    _circuit_id: String,
    _service_id: String,
    _starting_commit_hash: String,
    batch_results: Vec<BatchExecutionResult<OneBatch>>,
}

impl BatchContext {
    pub fn new(circuit_id: String, service_id: String, starting_commit_hash: String) -> Self {
        BatchContext {
            _circuit_id: circuit_id,
            _service_id: service_id,
            _starting_commit_hash: starting_commit_hash,
            batch_results: Vec::new(),
        }
    }
}

/// This implementation could go into Scabbard
impl PublisherContext<OneBatch> for BatchContext {
    fn add_batch_results(&mut self, batch_results: Vec<BatchExecutionResult<OneBatch>>) {
        self.batch_results.extend(batch_results)
    }

    fn compute_state_id(
        &mut self,
        _txn_receipts: &[TransactionReceipt],
    ) -> Result<String, InternalError> {
        Ok("1234".to_string())
    }
}

#[derive(Clone, Debug)]
pub struct OneBatch {
    id: String,
}

impl Batch for OneBatch {
    fn id(&self) -> String {
        self.id.to_string()
    }
}

#[derive(Clone, Debug)]
pub struct PublishBatchResultCreatorFactory {}

impl PublishBatchResultCreatorFactory {
    pub fn new() -> Self {
        PublishBatchResultCreatorFactory {}
    }
}

impl PublishedResultCreatorFactory<OneBatch, BatchContext, PublishBatchResult>
    for PublishBatchResultCreatorFactory
{
    fn new_creator(
        &self,
    ) -> Result<
        Box<dyn PublishedResultCreator<OneBatch, BatchContext, PublishBatchResult>>,
        InternalError,
    > {
        Ok(Box::new(PublishBatchResultCreator {}))
    }
}

#[derive(Clone, Debug)]
pub struct PublishBatchResultCreator {}

impl PublishedResultCreator<OneBatch, BatchContext, PublishBatchResult>
    for PublishBatchResultCreator
{
    fn create(
        &self,
        _context: BatchContext,
        batch_results: Vec<BatchExecutionResult<OneBatch>>,
        resulting_state_root: String,
    ) -> Result<PublishBatchResult, InternalError> {
        if batch_results.len() != 1 {
            // did not receive the expected number of BatchExecutionResults
            return Err(InternalError);
        }

        let receipts = batch_results[0].receipts.to_vec();

        Ok(PublishBatchResult {
            state_root_hash: resulting_state_root,
            receipts,
        })
    }
}

pub struct OneBatchVerifierFactory {}

impl OneBatchVerifierFactory {
    pub fn new() -> Self {
        OneBatchVerifierFactory {}
    }
}

impl BatchVerifierFactory<OneBatch, BatchContext> for OneBatchVerifierFactory {
    fn start(
        &mut self,
        context: BatchContext,
    ) -> Result<Box<dyn BatchVerifier<OneBatch, BatchContext>>, InternalError> {
        Ok(Box::new(OneBatchVerifier {
            _context: context,
            batch: None,
        }))
    }
}

#[derive(Clone, Debug)]
pub struct OneBatchVerifier {
    _context: BatchContext,
    batch: Option<OneBatch>,
}

impl BatchVerifier<OneBatch, BatchContext> for OneBatchVerifier {
    fn add_batch(&mut self, batch: OneBatch) -> Result<(), InternalError> {
        self.batch = Some(batch);
        Ok(())
    }

    fn finalize(&mut self) -> Result<Vec<BatchExecutionResult<OneBatch>>, InternalError> {
        Ok(vec![BatchExecutionResult {
            /// The `BatchPair` which was executed.
            batch: self.batch.take().ok_or(InternalError)?,

            /// The receipts for each transaction in the batch.
            receipts: vec![TransactionReceipt],
        }])
    }

    fn cancel(&mut self) -> Result<(), InternalError> {
        Ok(())
    }
}

pub struct BatchIter {
    batch: Option<OneBatch>,
}

impl BatchIter {
    pub fn new() -> Self {
        Self {
            batch: Some(OneBatch {
                id: "new-batch".to_string(),
            }),
        }
    }
}

impl PendingBatches<OneBatch> for BatchIter {
    fn next(&mut self) -> Result<Option<OneBatch>, InternalError> {
        if self.batch.is_none() {
            std::thread::sleep(std::time::Duration::from_secs(3));
        }
        Ok(self.batch.take())
    }
}
