#[macro_use]
extern crate bitfield;

use gtk::prelude::*;
use gtk::gio::ApplicationFlags;

mod backend;
mod capture;
mod compact_index;
mod data_stream;
mod decoder;
mod expander;
mod id;
mod index_stream;
mod model;
mod rcu;
mod row_data;
mod stream;
mod tree_list_model;
mod ui;
mod usb;
mod util;
mod vec_map;

#[cfg(any(feature="test-ui-replay", feature="record-ui-test"))]
mod record_ui;

use ui::{
    activate,
    display_error,
    stop_cynthion
};

fn main() {
    let application = gtk::Application::new(
        Some("com.greatscottgadgets.packetry"),
        ApplicationFlags::NON_UNIQUE
    );
    application.connect_activate(|app| display_error(activate(app)));
    application.run_with_args::<&str>(&[]);
    display_error(stop_cynthion());
}
