use crate::Error;
use std::{
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const IDLE_LIMIT: Duration = Duration::from_secs(30);

struct State {
    idle: Vec<(redis::aio::MultiplexedConnection, Instant)>,
    open: u32,
}
pub(crate) struct Pool {
    client: redis::Client,
    max: u32,
    permits: Arc<Semaphore>,
    state: Mutex<State>,
    timeout: Duration,
}
struct Reservation {
    pool: Arc<Pool>,
    armed: bool,
}
impl Reservation {
    fn release(mut self) {
        self.armed = false;
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.open = state.open.saturating_sub(1);
    }
}
pub(crate) struct Checkout {
    conn: Option<redis::aio::MultiplexedConnection>,
    reuse: bool,
    pool: Arc<Pool>,
    _permit: OwnedSemaphorePermit,
}
impl Checkout {
    pub(crate) async fn query<T: redis::FromRedisValue>(
        &mut self,
        cmd: &redis::Cmd,
    ) -> Result<T, Error> {
        let Some(conn) = self.conn.as_mut() else {
            return Err(Error::Unavailable);
        };
        match cmd.query_async(conn).await {
            Ok(value) => Ok(value),
            Err(error) => {
                self.reuse = false;
                Err(map_redis(error))
            }
        }
    }
    pub(crate) async fn query_pipe<T: redis::FromRedisValue>(
        &mut self,
        pipe: &redis::Pipeline,
    ) -> Result<T, Error> {
        let Some(conn) = self.conn.as_mut() else {
            return Err(Error::Unavailable);
        };
        match pipe.query_async(conn).await {
            Ok(value) => Ok(value),
            Err(error) => {
                self.reuse = false;
                Err(map_redis(error))
            }
        }
    }
}
impl Drop for Checkout {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.reuse {
            state.idle.push((conn, Instant::now()));
        } else {
            state.open = state.open.saturating_sub(1);
        }
    }
}
pub(crate) fn map_redis(error: redis::RedisError) -> Error {
    if error.is_timeout() {
        Error::Timeout
    } else {
        Error::Unavailable
    }
}
impl Pool {
    pub(crate) fn new(client: redis::Client, max: u32, timeout: Duration) -> Self {
        Self {
            client,
            max,
            permits: Arc::new(Semaphore::new(usize::try_from(max).unwrap_or(1))),
            state: Mutex::new(State {
                idle: Vec::new(),
                open: 0,
            }),
            timeout,
        }
    }
    pub(crate) fn status(&self) -> (u32, usize) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        (state.open, state.idle.len())
    }
    fn take_idle(&self) -> Option<redis::aio::MultiplexedConnection> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while let Some((conn, at)) = state.idle.pop() {
            if at.elapsed() < IDLE_LIMIT {
                return Some(conn);
            }
            state.open = state.open.saturating_sub(1);
        }
        None
    }
    fn reserve(self: &Arc<Self>) -> Result<Reservation, Error> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.open >= self.max {
            return Err(Error::Busy);
        }
        state.open = state.open.saturating_add(1);
        Ok(Reservation {
            pool: Arc::clone(self),
            armed: true,
        })
    }
    async fn connect(&self) -> Result<redis::aio::MultiplexedConnection, Error> {
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(self.timeout)
            .set_response_timeout(self.timeout);
        match tokio::time::timeout(
            self.timeout,
            self.client
                .get_multiplexed_async_connection_with_config(&config),
        )
        .await
        {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(error)) => Err(map_redis(error)),
            Err(_) => Err(Error::Timeout),
        }
    }
    pub(crate) async fn checkout(self: &Arc<Self>) -> Result<Checkout, Error> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        if let Some(conn) = self.take_idle() {
            return Ok(Checkout {
                conn: Some(conn),
                reuse: true,
                pool: Arc::clone(self),
                _permit: permit,
            });
        }
        let reservation = self.reserve()?;
        let conn = self.connect().await?;
        reservation.release();
        Ok(Checkout {
            conn: Some(conn),
            reuse: true,
            pool: Arc::clone(self),
            _permit: permit,
        })
    }
}
