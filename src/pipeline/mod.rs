/*!
 * Using the type state pattern with HList to define pipeline states.
 * It's admittedly over-engineered given the CLI handles source selection,
 * but it serves as a playground for enforcing builder order at compile time.
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

// HList tracking configured data stores at the type level
pub struct Nil;
pub struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

pub struct Postgres;
pub struct S3;

// Type-level selector/index pattern for implementation disambiguation
pub struct Here;
pub struct There<Index>(PhantomData<Index>);

trait Contains<T, Index> {}

impl<T, Tail> Contains<T, Here> for Cons<T, Tail> {}

impl<T, Head, Tail, Index> Contains<T, There<Index>> for Cons<Head, Tail> where
    Tail: Contains<T, Index>
{
}

// Markers for the type state machine
pub struct Start;
pub struct PersistState<Stores>(PhantomData<Stores>);
pub struct LoadState<Stores>(PhantomData<Stores>);
pub struct Compete;

pub struct Pipeline<State> {
    stages: Vec<Box<dyn Stage>>,
    state: PhantomData<State>,
}

impl Pipeline<Start> {
    pub fn builder() -> Pipeline<PersistState<Nil>> {
        Pipeline {
            stages: vec![Box::new(LoadRawData), Box::new(ExpandDataset)],
            state: PhantomData,
        }
    }
}

impl<Stores> Pipeline<PersistState<Stores>> {
    pub fn with_postgres_persistence(self) -> Pipeline<PersistState<Cons<Postgres, Stores>>> {
        self.add_persistence_stage(PersistPostgres)
    }

    pub fn with_s3_persistence(self) -> Pipeline<PersistState<Cons<S3, Stores>>> {
        self.add_persistence_stage(PersistS3)
    }

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

// Enforce that retrieval layers come after persistence
impl<Stores> Pipeline<PersistState<Stores>> {
    pub fn with_postgres_retrieval(self) -> Pipeline<LoadState<Cons<Postgres, Stores>>> {
        add_load_stage(self.stages, LoadFromPostgres)
    }

    pub fn with_s3_retrieval(self) -> Pipeline<LoadState<Cons<S3, Stores>>> {
        add_load_stage(self.stages, LoadFromS3)
    }
}

// Demonstration: we can chain retrieval layers (even if the CLI only uses one)
impl<Stores> Pipeline<LoadState<Cons<S3, Stores>>> {
    pub fn with_postgres_retrieval(self) -> Pipeline<LoadState<Cons<Postgres, Stores>>> {
        add_load_stage(self.stages, LoadFromPostgres)
    }
}
impl<Stores> Pipeline<LoadState<Cons<Postgres, Stores>>> {
    pub fn with_s3_retrieval(self) -> Pipeline<LoadState<Cons<S3, Stores>>> {
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
// Finalise the pipeline for execution
impl<Stores> Pipeline<LoadState<Stores>> {
    pub fn finish(self) -> Pipeline<Compete> {
        Pipeline {
            stages: self.stages,
            state: PhantomData,
        }
    }
}

impl<Compete> Pipeline<Compete> {
    pub async fn run(self, mut ctx: Context) -> Result<Context> {
        for stage in self.stages {
            ctx = stage.run(ctx).await?;
        }
        Ok(ctx)
    }
}
