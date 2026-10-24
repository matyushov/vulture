//! Tracked symbols per exchange, managed at runtime (V1-11).
//!
//! - [`policy`]: the policy model and the set computations (pure functions).
//! - [`state`]: the versioned state file `<data_dir>/state.json` with the owner's policies.
//! - [`audit`]: the size-bounded audit log of changes.
//! - [`channel`]: the watch-based link between the control and a connector.
//! - [`manager`]: validation, persistence, audit and metrics; the backend of `Control.SetSymbolPolicy`.
//!
//! Removed symbols simply stop producing events: no `Gap` is published for them, because untracking is not an
//! outage. The history of a period in which a symbol was not tracked does not exist and cannot be backfilled
//! (open interest is a snapshot, a liquidation missed is gone).

pub mod audit;
pub mod channel;
pub mod manager;
pub mod policy;
pub mod state;

pub use audit::{AUDIT_FILE, AuditLog, AuditRecord};
pub use channel::SymbolChannel;
pub use manager::{PolicyView, SetError, SetOutcome, SymbolManager};
pub use policy::{
    MAX_POLICY_SYMBOLS, Mode, PolicyError, SetDiff, SymbolPolicy, Universe, normalize_symbol_name,
};
pub use state::{STATE_FILE, STATE_VERSION, State, StateError, StateStore};
