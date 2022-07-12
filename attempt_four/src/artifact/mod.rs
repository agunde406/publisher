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

use super::error::InternalError;

pub trait ArtifactCreator {
    type Context;
    type Input;
    type Artifact;

    fn create(
        &self,
        context: Self::Context,
        input: Self::Input,
    ) -> Result<Self::Artifact, InternalError>;
}

pub trait ArtifactCreatorFactory {
    type Context;
    type Input;
    type Artifact;

    // clippy complains that the return type for new_creator is too complex, but breaking into
    // types runs into the error "associated type defaults are unstable"
    #[allow(clippy::type_complexity)]
    fn new_creator(
        &self,
    ) -> Result<
        Box<
            dyn ArtifactCreator<
                Context = Self::Context,
                Input = Self::Input,
                Artifact = Self::Artifact,
            >,
        >,
        InternalError,
    >;
}
