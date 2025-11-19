use std::fmt;

#[derive(Debug)]
pub(crate) enum PipelineBuildError {
    AtLeastOnePersistStageMustBeSpecified,
    PersistAndLoadStoresMustBeTheSame,
    ArgsLoadStoreNotAvailable(String),
}

impl fmt::Display for PipelineBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineBuildError::AtLeastOnePersistStageMustBeSpecified => {
                write!(f, "At least one persist stage must be specified")
            }
            PipelineBuildError::PersistAndLoadStoresMustBeTheSame => {
                write!(f, "Persist and load stores must be the same")
            }
            PipelineBuildError::ArgsLoadStoreNotAvailable(store) => {
                write!(
                    f,
                    "Store {} specified via CLI has not been added to pipeline builder",
                    store
                )
            }
        }
    }
}

impl std::error::Error for PipelineBuildError {}

#[derive(Debug)]
pub struct PipelineValidationError {
    pub errors: Vec<PipelineBuildError>,
}

impl fmt::Display for PipelineValidationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let error_str = self
            .errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Pipeline validation error: {}", error_str)
    }
}

impl std::error::Error for PipelineValidationError {}
