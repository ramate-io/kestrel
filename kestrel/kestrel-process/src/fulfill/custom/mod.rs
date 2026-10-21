use crate::fulfill::{Fulfill, FulfillError};
use kestrel_state::WritableState;
use std::future::Future;
use tokio::sync::mpsc::{Receiver, Sender};

pub trait CustomProcessor<T> {
	fn process_receiver(
		&self,
		receiver: &mut Receiver<String>,
	) -> impl Future<Output = Result<Option<T>, FulfillError>> + Send;
}

/// Fulfills when a piped line contains `needle`. Blocks on recv so `Fulfill::run` does not spin.
pub struct ContainsLine {
	pub needle: String,
}

impl ContainsLine {
	pub fn new(needle: impl Into<String>) -> Self {
		Self { needle: needle.into() }
	}
}

impl CustomProcessor<String> for ContainsLine {
	fn process_receiver(
		&self,
		receiver: &mut Receiver<String>,
	) -> impl Future<Output = Result<Option<String>, FulfillError>> + Send {
		let needle = self.needle.clone();
		async move {
			while let Some(line) = receiver.recv().await {
				if line.contains(&needle) {
					return Ok(Some(line));
				}
			}
			Ok(None)
		}
	}
}

/// Custom struct that fulfills requests using a receiver and an async closure.
pub struct Custom<T, P>
where
	T: Clone + Send + Sync + 'static,
	P: CustomProcessor<T> + Send + Sync + 'static,
{
	sender: Sender<String>,
	receiver: Receiver<String>,
	state: WritableState<T>,
	task: P,
}

impl<T, P> Custom<T, P>
where
	T: Clone + Send + Sync + 'static,
	P: CustomProcessor<T> + Send + Sync + 'static,
{
	/// Creates a new Custom processor.
	pub fn new(state: WritableState<T>, task: P) -> Self {
		let (sender, receiver) = tokio::sync::mpsc::channel(100);

		Self { sender, receiver, state, task }
	}
}

impl<T, P> Fulfill<T> for Custom<T, P>
where
	T: Clone + Send + Sync + 'static,
	P: CustomProcessor<T> + Send + Sync + 'static,
{
	/// Gets the sender that will be used to fulfill the request via the pipe.
	fn sender(&self) -> Result<Sender<String>, FulfillError> {
		Ok(self.sender.clone()) // Cloning Sender is allowed
	}

	/// Gets the writable state value which is supposed to be fulfilled.
	fn dependency(&self) -> Result<WritableState<T>, FulfillError> {
		Ok(self.state.clone()) // Assuming WritableState<T> implements Clone
	}

	/// Attempts to get the value to fulfill the request.
	fn try_get(&mut self) -> impl Future<Output = Result<Option<T>, FulfillError>> + Send {
		async move { self.task.process_receiver(&mut self.receiver).await }
	}
}
