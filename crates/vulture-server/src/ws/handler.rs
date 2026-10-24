//! The exchange-specific part of a WebSocket session.

use super::topics::{TopicOp, TopicRequest};

/// A data frame received from the exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// UTF-8 text frame.
    Text(String),
    /// Binary frame.
    Binary(Vec<u8>),
}

impl Frame {
    /// The text, if this is a text frame.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Frame::Text(t) => Some(t),
            Frame::Binary(_) => None,
        }
    }
}

/// How the manager treats a frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameClass {
    /// Payload for the consumer. `liveness` marks frames of the always-ticking stream that proves the
    /// subscription works (for example Binance `!markPrice@arr@1s`); they are forwarded as well.
    Data {
        /// Whether the frame counts as liveness data.
        liveness: bool,
    },
    /// Application-level pong (text `pong`, `{"op":"pong"}`); swallowed.
    Pong,
    /// Subscription acknowledgement; swallowed. `ok: false` fails the session and triggers a retry.
    Ack {
        /// Whether the exchange accepted the subscription.
        ok: bool,
    },
    /// Protocol message that needs an answer (Deribit `test_request`); the text is sent back, nothing is
    /// forwarded.
    Reply(String),
    /// Response to a runtime topic request (see [`WsHandler::topic_requests`]); swallowed. A failure makes the
    /// session reject or split the request, it does not end the session.
    TopicResponse {
        /// Correlation id of the request, if the exchange echoed one.
        id: Option<String>,
        /// Whether the exchange accepted the request.
        ok: bool,
        /// The exchange's message (used to find the offending topic).
        detail: String,
    },
    /// Not interesting (welcome banners, info notices); swallowed.
    Ignore,
}

/// Implemented by each connector for its exchange. Called from the session task, so it must be quick and
/// must not block.
pub trait WsHandler: Send + Sync + 'static {
    /// Text frames sent right after every (re)connect, in order.
    fn subscribe_messages(&self) -> Vec<String>;

    /// Classifies an incoming frame.
    fn classify(&self, frame: &Frame) -> FrameClass;

    /// Request frames for a runtime subscription change, split to respect the exchange's message limits. The
    /// default builds none, which is right for connectors with a fixed subscription
    /// ([`WsHandler::subscribe_messages`]). `next_id` is the next free correlation id; the handler advances it
    /// by one per request.
    fn topic_requests(
        &self,
        _op: TopicOp,
        _topics: &[String],
        _next_id: &mut u64,
    ) -> Vec<TopicRequest> {
        Vec::new()
    }

    /// Key for deduplication during a make-before-break switch. Frames with equal keys are delivered once
    /// while two sessions overlap. `None` disables deduplication for the frame.
    fn dedup_key(&self, _frame: &Frame) -> Option<String> {
        None
    }
}
