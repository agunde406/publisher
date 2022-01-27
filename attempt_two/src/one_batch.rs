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
    PublisherContext, Transaction, TransactionReceipt,
};
use std::marker::PhantomData;

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
    batch_results: Vec<BatchExecutionResult<OneBatch, OneTransaction>>,
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
impl PublisherContext<OneBatch, OneTransaction> for BatchContext {
    fn add_batch_results(
        &mut self,
        batch_results: Vec<BatchExecutionResult<OneBatch, OneTransaction>>,
    ) {
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
    transactions: Vec<OneTransaction>,
}

impl Batch<OneTransaction> for OneBatch {
    fn id(&self) -> &str {
        &self.id
    }

    fn transactions(&self) -> &[OneTransaction] {
        &self.transactions
    }
}

#[derive(Clone, Debug)]
pub struct OneTransaction {
    id: String,
    payload: Vec<u8>,
    header: Vec<u8>,
}

impl Transaction for OneTransaction {
    fn id(&self) -> &str {
        &self.id
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn header(&self) -> &[u8] {
        &self.header
    }
}

#[derive(Clone, Debug)]
pub struct PublishBatchResultCreatorFactory {}

impl PublishBatchResultCreatorFactory {
    pub fn new() -> Self {
        PublishBatchResultCreatorFactory {}
    }
}

impl PublishedResultCreatorFactory<OneBatch, BatchContext, PublishBatchResult, OneTransaction>
    for PublishBatchResultCreatorFactory
{
    fn new_creator(
        &self,
    ) -> Result<
        Box<dyn PublishedResultCreator<OneBatch, BatchContext, PublishBatchResult, OneTransaction>>,
        InternalError,
    > {
        Ok(Box::new(PublishBatchResultCreator {}))
    }
}

#[derive(Clone, Debug)]
pub struct PublishBatchResultCreator {}

impl PublishedResultCreator<OneBatch, BatchContext, PublishBatchResult, OneTransaction>
    for PublishBatchResultCreator
{
    fn create(
        &self,
        _context: BatchContext,
        batch_results: Vec<BatchExecutionResult<OneBatch, OneTransaction>>,
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

impl BatchVerifierFactory<OneBatch, BatchContext, OneTransaction> for OneBatchVerifierFactory {
    fn start(
        &mut self,
        context: BatchContext,
    ) -> Result<Box<dyn BatchVerifier<OneBatch, BatchContext, OneTransaction>>, InternalError> {
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

impl BatchVerifier<OneBatch, BatchContext, OneTransaction> for OneBatchVerifier {
    fn add_batch(&mut self, batch: OneBatch) -> Result<(), InternalError> {
        self.batch = Some(batch);
        Ok(())
    }

    fn finalize(
        &mut self,
    ) -> Result<Vec<BatchExecutionResult<OneBatch, OneTransaction>>, InternalError> {
        Ok(vec![BatchExecutionResult {
            /// The `BatchPair` which was executed.
            batch: self.batch.take().ok_or(InternalError)?,

            /// The receipts for each transaction in the batch.
            receipts: vec![TransactionReceipt],
            _transaction: PhantomData,
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
                transactions: vec![OneTransaction {
                    id: "new-txn".to_string(),
                    payload: "payload".as_bytes().to_vec(),
                    header: "header".as_bytes().to_vec(),
                }],
            }),
        }
    }
}

impl PendingBatches<OneBatch, OneTransaction> for BatchIter {
    fn next(&mut self) -> Result<Option<OneBatch>, InternalError> {
        Ok(self.batch.take())
    }
}
