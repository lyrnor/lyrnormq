//! Read-only, bounded observations of the installed broker state.
mod handle;
#[cfg(test)]
mod tests;
mod types;
pub use handle::ManagementHandle;
pub(crate) use handle::{Observation, Source};
pub use types::{Overview, Page, Query, RetainedView, SessionView, SubscriptionView};
