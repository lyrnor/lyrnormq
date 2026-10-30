//! Network handshake output ordering; provider state lives on the worker.
use super::*;

pub(super) async fn handshake_reply(
    context: &mut Context<'_>,
    response: Reply,
    version: ProtocolVersion,
    mut client: String,
) -> io::Result<bool> {
    let frame = match response {
        Reply::AuthContinue(frame) => {
            *context.authenticating = true;
            send_handshake(context, frame).await?;
            return Ok(false);
        }
        Reply::Connected(present) => accepted_connack(version, present, context.config, None),
        Reply::Assigned {
            present,
            client: resolved,
        } => {
            client = resolved;
            accepted_connack(version, present, context.config, Some(&client))
        }
        Reply::Authenticated {
            client: resolved,
            frame,
        } => {
            client = resolved;
            frame
        }
        Reply::Rejected(reason) => {
            terminal_frame(
                context,
                connack(version, false, reason.code(), context.config),
            );
            *context.flush = true;
            return Err(failure(ReasonCode::NOT_AUTHORIZED, "CONNECT refused"));
        }
        Reply::FrameRejected(reason) => {
            if context.peer_maximum.load(Ordering::Acquire) >= 5 {
                terminal_frame(
                    context,
                    connack(version, false, reason.value(), context.config),
                );
                *context.flush = true;
            }
            return Err(failure(reason, "authentication or CONNECT refused"));
        }
        _ => return Err(io::Error::other("invalid handshake response")),
    };
    *context.identity = Some(client);
    context.worker.adopt_connection(context.generation);
    *context.connected = true;
    *context.authenticating = false;
    // CONNACK must reach the socket before terminal errors from pipelined input.
    send_handshake(context, frame).await?;
    context.worker.release_handshake();
    Ok(true)
}
async fn send_handshake(context: &Context<'_>, frame: Frame) -> io::Result<()> {
    let (written_tx, written_rx) = oneshot::channel();
    let mut message = outgoing(frame, context.credits)?;
    message.written = Some(written_tx);
    context
        .output
        .send(message)
        .await
        .map_err(io::Error::other)?;
    written_rx
        .await
        .map_err(|_| failure(ReasonCode::SERVER_UNAVAILABLE, "handshake write failed"))
}

/// Drop the caller future on timeout/shutdown: its guard cancels provider output,
/// while the still-running blocking job retains its global CPU permit.
pub(super) async fn request(
    context: &Context<'_>,
    operation: Operation,
    deadline: Instant,
) -> io::Result<Reply> {
    let mut shutdown = context.shutdown.clone();
    let mut cancel = context.cancel.clone();
    let operation = context.worker.request(operation);
    tokio::pin!(operation);
    loop {
        if *shutdown.borrow() || *cancel.borrow() {
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "authentication cancelled",
            ));
        }
        tokio::select! {
            biased;
            result=shutdown.changed()=>{if result.is_err() || *shutdown.borrow(){return Err(failure(ReasonCode::SERVER_UNAVAILABLE,"authentication shutdown"));}},
            result=cancel.changed()=>{if result.is_err() || *cancel.borrow(){return Err(failure(ReasonCode::SERVER_UNAVAILABLE,"authentication cancelled"));}},
            result=tokio::time::timeout_at(deadline,&mut operation)=>return result.map_err(|_|failure(ReasonCode::NOT_AUTHORIZED,"authentication deadline exceeded"))?,
        }
    }
}
