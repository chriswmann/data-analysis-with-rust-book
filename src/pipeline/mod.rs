/*! I'll use the type state pattern with HList and index patterns,
 * to define the states here.
 * This is a bit daft since what I'll actually do is add both S3 and PG
 * to the pipeline as sinks and sources of data. The CLI will then allow
 * the user to specify which source to use. But I'll keep the type state pattern
 * to enforce the order of the builder and to demonstrate a more advanced use of it.
*/
#![allow(dead_code)]
pub mod context;
pub mod stage;
pub mod stages;

use crate::pipeline::stages::{
    ensure_raw::LoadRawData, expand_dataset::ExpandDataset, load_from_postgres::LoadFromPostgres,
    load_from_s3::LoadFromS3, persist_postgres::PersistPostgres, persist_s3::PersistS3,
};
use crate::pipeline::{context::Context, stage::Stage};
use anyhow::Result;

use std::marker::PhantomData;

// I'll keep the data store list at the type level
// Top level stores Hlist
pub(crate) struct Nil;
pub(crate) struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

// Available stores
pub(crate) struct Postgres;
pub(crate) struct S3;

// Relationship check
// Using the type-level selector/index pattern to
// disambiguate implementations
pub(crate) struct Here;
pub(crate) struct There<Index>(PhantomData<Index>);

trait Contains<T, Index> {}

impl<T, Tail> Contains<T, Here> for Cons<T, Tail> {}

impl<T, Head, Tail, Index> Contains<T, There<Index>> for Cons<Head, Tail> where
    Tail: Contains<T, Index>
{
}

// State markers
pub(crate) struct Start;
pub(crate) struct PersistState<Stores>(PhantomData<Stores>);
pub(crate) struct LoadState<Stores>(PhantomData<Stores>);
pub(crate) struct Compete;

pub(crate) struct Pipeline<State> {
    stages: Vec<Box<dyn Stage>>,
    state: PhantomData<State>,
}

// Build the pipeline
impl Pipeline<Start> {
    pub(crate) fn builder() -> Pipeline<PersistState<Nil>> {
        Pipeline {
            stages: vec![Box::new(LoadRawData), Box::new(ExpandDataset)],
            state: PhantomData,
        }
    }
}

// Add persistence layer(s) to the pipeline
impl<Stores> Pipeline<PersistState<Stores>> {
    pub(crate) fn with_postgres_persistence(
        self,
    ) -> Pipeline<PersistState<Cons<Postgres, Stores>>> {
        self.add_persistence_stage(PersistPostgres)
    }

    pub(crate) fn with_s3_persistence(self) -> Pipeline<PersistState<Cons<S3, Stores>>> {
        self.add_persistence_stage(PersistS3)
    }

    // Helper function to add a persistence stage to the pipeline
    fn add_persistence_stage<S, Store>(
        self,
        store: S,
    ) -> Pipeline<PersistState<Cons<Store, Stores>>>
    where
        S: Stage + 'static,
    {
        let mut stages = self.stages;
        stages.push(Box::new(store));
        Pipeline {
            stages: stages,
            state: PhantomData,
        }
    }
}

// Add first retrieval layer to the pipeline
impl<Stores> Pipeline<PersistState<Stores>> {
    pub(crate) fn with_postgres_retrieval(self) -> Pipeline<LoadState<Cons<Postgres, Stores>>> {
        add_load_stage(self.stages, LoadFromPostgres)
    }

    pub(crate) fn with_s3_retrieval(self) -> Pipeline<LoadState<Cons<S3, Stores>>> {
        add_load_stage(self.stages, LoadFromS3)
    }
}

// Add second retrieval layer to the pipeline.
// We will only use one retrieval layer, as specified by the CLI arguments, but
// but we'll do it this way to demonstrate the type state pattern.
impl<Stores> Pipeline<LoadState<Cons<S3, Stores>>> {
    pub(crate) fn with_postgres_retrieval(self) -> Pipeline<LoadState<Cons<Postgres, Stores>>> {
        add_load_stage(self.stages, LoadFromPostgres)
    }
}
impl<Stores> Pipeline<LoadState<Cons<Postgres, Stores>>> {
    pub(crate) fn with_s3_retrieval(self) -> Pipeline<LoadState<Cons<S3, Stores>>> {
        add_load_stage(self.stages, LoadFromS3)
    }
}

fn add_load_stage<S, Store, Stores>(
    mut stages: Vec<Box<dyn Stage>>,
    stage: S,
) -> Pipeline<LoadState<Cons<Store, Stores>>>
where
    S: Stage + 'static,
{
    stages.push(Box::new(stage));
    Pipeline {
        stages: stages,
        state: PhantomData,
    }
}
// Finish by adding the Complete state
impl<Stores> Pipeline<LoadState<Stores>> {
    pub(crate) fn finish(self) -> Pipeline<Compete> {
        Pipeline {
            stages: self.stages,
            state: PhantomData,
        }
    }
}

impl<Compete> Pipeline<Compete> {
    pub(crate) async fn run(self, mut ctx: Context) -> Result<Context> {
        for stage in self.stages {
            ctx = stage.run(ctx).await?;
        }
        Ok(ctx)
    }
}
