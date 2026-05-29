use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::atomic::Ordering;

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use crate::c_api::ON_LOG;

/// Tracing [`Layer`] that forwards every event to the Go log callback
/// registered via `reticulum_set_log_callback`.
///
/// Level encoding (matches `log` crate ordinals):
///   1 = Error, 2 = Warn, 3 = Info, 4 = Debug, 5 = Trace
pub struct CLogLayer;

#[derive(Default)]
struct MsgVisitor {
    message: String,
    module: String,
    // file: String,
}

impl tracing::field::Visit for MsgVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "message"                    => self.message = value.to_string(),
            "log.module_path"            => self.module = value.to_string(),
            // "log.file"                   => self.file = value.to_string(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

impl<S: Subscriber> Layer<S> for CLogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let ptr = ON_LOG.load(Ordering::Relaxed);
        if ptr == 0 {
            return;
        }

        let level: u8 = match *event.metadata().level() {
            Level::ERROR => 1,
            Level::WARN  => 2,
            Level::INFO  => 3,
            Level::DEBUG => 4,
            Level::TRACE => 5,
        };
        let mut visitor = MsgVisitor::default();
        event.record(&mut visitor);

        let (Ok(c_target), Ok(c_msg)) = (
            CString::new(visitor.module),
            CString::new(visitor.message),
        ) else {
            return;
        };

        let f: extern "C" fn(u8, *const c_char, *const c_char) =
            unsafe { std::mem::transmute(ptr) };
        f(level, c_target.as_ptr(), c_msg.as_ptr());
    }
}
