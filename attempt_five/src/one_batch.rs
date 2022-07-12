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
    Artifact, ArtifactCreator, ArtifactCreatorFactory, Batch, BatchExecutionResult, BatchVerifier,
    BatchVerifierFactory, InternalError, PendingBatches, PublisherContext, Transaction,
};

#[derive(Clone, Debug)]
pub struct PublishBatchResult {
    pub state_root_hash: String,
    pub receipts: Vec<TransactionReceipt>,
}

impl Artifact for PublishBatchResult {
    type Identifier = String;

    fn artifact_id(&self) -> &Self::Identifier {
        &self.state_root_hash
    }
}

#[derive(Clone, Debug)]
pub struct BatchContext {
    _circuit_id: String,
    _service_id: String,
    _starting_commit_hash: String,
    batch_results: Vec<OneBatchExecutionResult>,
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

    fn add_batch_results(&mut self, batch_results: Vec<OneBatchExecutionResult>) {
        self.batch_results.extend(batch_results)
    }

    fn compute_state_id(
        &mut self,
        _txn_receipts: &[TransactionReceipt],
    ) -> Result<String, InternalError> {
        Ok("1234".to_string())
    }
}

/// This implementation could go into Scabbard
impl PublisherContext for BatchContext {}

#[derive(Clone, Debug)]
pub struct OneBatch {
    id: String,
    transactions: Vec<OneTransaction>,
}

impl Batch for OneBatch {
    type Transaction = OneTransaction;
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

impl ArtifactCreatorFactory for PublishBatchResultCreatorFactory {
    type ArtifactCreator = Box<
        dyn ArtifactCreator<
            Context = BatchContext,
            Input = Vec<OneBatchExecutionResult>,
            Artifact = PublishBatchResult,
        >,
    >;

    fn new_creator(&self) -> Result<Self::ArtifactCreator, InternalError> {
        Ok(Box::new(PublishBatchResultCreator {}))
    }
}

#[derive(Clone, Debug)]
pub struct PublishBatchResultCreator {}

impl ArtifactCreator for PublishBatchResultCreator {
    type Context = BatchContext;
    type Artifact = PublishBatchResult;
    type Input = Vec<OneBatchExecutionResult>;

    fn create(
        &self,
        context: &mut BatchContext,
        input: Vec<OneBatchExecutionResult>,
    ) -> Result<PublishBatchResult, InternalError> {
        let mut txn_receipts = Vec::new();

        if input.len() != 1 {
            // did not receive the expected number of BatchExecutionResults
            return Err(InternalError);
        }

        for batch_result in input.iter() {
            txn_receipts.append(&mut batch_result.receipts.to_vec())
        }

        context.add_batch_results(input.clone());

        let state_root = context.compute_state_id(&txn_receipts)?;

        let receipts = input[0].receipts.to_vec();

        Ok(PublishBatchResult {
            state_root_hash: state_root,
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

impl BatchVerifierFactory for OneBatchVerifierFactory {
    type Batch = OneBatch;
    type Context = BatchContext;
    type ExecutionResult = OneBatchExecutionResult;
    fn start(
        &mut self,
        context: BatchContext,
    ) -> Result<
        Box<
            dyn BatchVerifier<
                Batch = OneBatch,
                Context = BatchContext,
                ExecutionResult = OneBatchExecutionResult,
            >,
        >,
        InternalError,
    > {
        Ok(Box::new(OneBatchVerifier {
            _context: context,
            batch: None,
        }))
    }
}

/// This struct is in sawtooth-lib
#[derive(Debug, Clone)]
pub struct TransactionReceipt;

/// Result of executing a batch.
#[derive(Debug, Clone)]
pub struct OneBatchExecutionResult {
    /// The `BatchPair` which was executed.
    pub batch: OneBatch,

    /// The receipts for each transaction in the batch.
    pub receipts: Vec<TransactionReceipt>,
}

impl BatchExecutionResult for OneBatchExecutionResult {}

#[derive(Clone, Debug)]
pub struct OneBatchVerifier {
    _context: BatchContext,
    batch: Option<OneBatch>,
}

impl BatchVerifier for OneBatchVerifier {
    type Batch = OneBatch;
    type Context = BatchContext;
    type ExecutionResult = OneBatchExecutionResult;

    fn add_batch(&mut self, batch: OneBatch) -> Result<(), InternalError> {
        self.batch = Some(batch);
        Ok(())
    }

    fn finalize(&mut self) -> Result<Vec<OneBatchExecutionResult>, InternalError> {
        Ok(vec![OneBatchExecutionResult {
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
                transactions: vec![OneTransaction {
                    id: "new-txn".to_string(),
                    payload: "payload".as_bytes().to_vec(),
                    header: "header".as_bytes().to_vec(),
                }],
            }),
        }
    }
}

impl PendingBatches<OneBatch> for BatchIter {
    fn next(&mut self) -> Result<Option<OneBatch>, InternalError> {
        Ok(self.batch.take())
    }
}
