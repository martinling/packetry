#[macro_use]
extern crate bitfield;
use thiserror::Error;

mod backend;
mod model;
mod row_data;
mod expander;
mod tree_list_model;

use std::cell::RefCell;
use std::fmt::Debug;
use std::fs::File;
use std::sync::{Arc, Mutex};

use gtk::gio::ListModel;
use gtk::glib::Object;
use gtk::{
    prelude::*,
    Application,
    ApplicationWindow,
    MessageDialog,
    DialogFlags,
    MessageType,
    ButtonsType,
    ListItem,
    ListView,
    SignalListItemFactory,
    SingleSelection,
    Orientation,
};

use pcap_file::{PcapError, pcap::PcapReader};

use model::{GenericModel, TrafficModel, DeviceModel};
use row_data::GenericRowData;
use expander::ExpanderWrapper;
use tree_list_model::ModelError;

mod capture;
use capture::{Capture, CaptureError, ItemSource};

mod decoder;
use decoder::Decoder;

mod id;
mod file_vec;
mod hybrid_index;
mod usb;
mod vec_map;

#[cfg(any(test, feature="record-ui-test"))]
mod record_ui;

#[cfg(feature="record-ui-test")]
use {
    std::rc::Rc,
    std::path::Path,
    record_ui::{UIAction, create_logfile, log_action},
};

thread_local!(
    static MODELS: RefCell<Vec<Object>>  = RefCell::new(Vec::new());
    static LUNA: RefCell<Option<backend::luna::LunaCapture>> = RefCell::new(None);
    static WINDOW: RefCell<Option<ApplicationWindow>> = RefCell::new(None);
);

fn create_view<Item: 'static, Model, RowData>(
    capture: &Arc<Mutex<Capture>>,
    #[cfg(feature="record-ui-test")]
    logfile: Rc<RefCell<File>>
) -> ListView
    where
        Item: Copy + Debug,
        Model: GenericModel<Item> + IsA<ListModel> + IsA<Object>,
        RowData: GenericRowData<Item> + IsA<Object>,
        Capture: ItemSource<Item>
{
    let model = Model::new(capture.clone())
                      .expect("Failed to create model");

    MODELS.with(|models| models.borrow_mut().push(model.clone().upcast()));

    #[cfg(feature="record-ui-test")]
    log_action::<Model, RowData, Item>(
        &model,
        &logfile,
        UIAction::Startup());

    let cap_arc = capture.clone();
    let selection_model = SingleSelection::new(Some(&model));
    let factory = SignalListItemFactory::new();
    factory.connect_setup(move |_, list_item| {
        let expander = ExpanderWrapper::new();
        list_item.set_child(Some(&expander));
    });
    let bind = move |list_item: &ListItem| {
        let row = list_item
            .item()
            .or_bug("ListItem has no item")?
            .downcast::<RowData>()
            .or_bug("Item is not RowData")?;

        let expander_wrapper = list_item
            .child()
            .or_bug("ListItem has no child widget")?
            .downcast::<ExpanderWrapper>()
            .or_bug("Child widget is not an ExpanderWrapper")?;

        let expander = expander_wrapper.expander();
        match row.node() {
            Ok(node_ref) => {
                let node = node_ref.borrow();
                let summary = node.field(
                    &cap_arc, Box::new(Capture::summary));
                expander_wrapper.set_text(summary);
                let connectors = node.field(
                    &cap_arc, Box::new(Capture::connectors));
                expander_wrapper.set_connectors(connectors);
                expander.set_visible(node.expandable());
                expander.set_expanded(node.expanded());
                let model = model.clone();
                let node_ref = node_ref.clone();
                let list_item = list_item.clone();
                #[cfg(feature="record-ui-test")]
                let logfile = logfile.clone();
                let handler = expander.connect_expanded_notify(move |expander| {
                    let position = list_item.position();
                    let expanded = expander.is_expanded();
                    display_error(
                        model.set_expanded(&node_ref, position, expanded)
                            .map_err(PacketryError::Model));
                    #[cfg(feature="record-ui-test")]
                    log_action::<Model, RowData, Item>(
                        &model,
                        &logfile,
                        UIAction::SetExpanded(position, expanded));
                });
                expander_wrapper.set_handler(handler);
            },
            Err(msg) => {
                expander_wrapper.set_connectors("".to_string());
                expander_wrapper.set_text(format!("Error: {}", msg));
                expander.set_visible(false);
            }
        };
        Ok(())
    };
    let unbind = move |list_item: &ListItem| {
        let expander_wrapper = list_item
            .child()
            .or_bug("ListItem has no child widget")?
            .downcast::<ExpanderWrapper>()
            .or_bug("Child widget is not an ExpanderWrapper")?;
        let expander = expander_wrapper.expander();
        let handler = expander_wrapper
            .take_handler()
            .or_bug("ExpanderWrapper handler was not set")?;
        expander.disconnect(handler);
        Ok(())
    };
    factory.connect_bind(move |_, item| display_error(bind(item)));
    factory.connect_unbind(move |_, item| display_error(unbind(item)));
    ListView::new(Some(&selection_model), Some(&factory))
}

#[derive(Error, Debug)]
pub enum PacketryError {
    #[error("capture data error: {0}")]
    Capture(#[from] CaptureError),
    #[error("tree model error: {0}")]
    Model(#[from] ModelError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("pcap error: {0}")]
    Pcap(#[from] PcapError),
    #[error("LUNA error: {0}")]
    Luna(#[from] crate::backend::luna::Error),
    #[error("locking failed")]
    Lock,
    #[error("internal bug: {0}")]
    Bug(&'static str)
}

fn activate(application: &Application) -> Result<(), PacketryError> {
    let window = gtk::ApplicationWindow::builder()
        .default_width(320)
        .default_height(480)
        .application(application)
        .title("Packetry")
        .build();

    window.show();
    WINDOW.with(|win_opt| win_opt.replace(Some(window.clone())));

    let args: Vec<_> = std::env::args().collect();

    #[cfg(feature="record-ui-test")]
    let test_dir = {
        let file_path = Path::new(&args[1]);
        let dir = Path::new("./tests/ui/")
                .join(file_path
                    .file_stem()
                    .expect("No file name"));
        std::fs::create_dir_all(&dir)
            .expect("Failed to create test directory");
        std::fs::copy(file_path, dir.join(Path::new("capture.pcap")))
            .expect("Failed to copy capture file");
        dir
    };

    let mut cap = Capture::new()?;
    let mut decoder = Decoder::new(&mut cap)?;
    let capture = Arc::new(Mutex::new(cap));
    let app_capture = capture.clone();

    if args.len() > 1 {
        let pcap_file = File::open(&args[1])?;
        let pcap_reader = PcapReader::new(pcap_file)?;
        let mut cap = capture.lock().or(Err(PacketryError::Lock))?;
        for result in pcap_reader {
            match result {
                Ok(packet) => {
                    let decode_result =
                        decoder.handle_raw_packet(&mut cap, &packet.data);
                    if let Err(e) = decode_result {
                        display_error(Err(PacketryError::Capture(e)));
                        break;
                    }
                },
                Err(e) => {
                    display_error(Err(PacketryError::Pcap(e)));
                    break;
                }
            }
        }
        cap.print_storage_summary();
    } else {
        LUNA.with::<_, Result<(), PacketryError>>(|cell| {
            cell.borrow_mut().replace(
                backend::luna::LunaDevice::open()?.start()?
            );
            Ok(())
        })?;
        let update_capture = capture.clone();

        let mut update_routine = move || -> Result<(), PacketryError> {
            use PacketryError::*;

            let mut cap = update_capture.lock().or(Err(Lock))?;

            LUNA.with::<_, Result<(), PacketryError>>(|cell| {
                let mut borrow = cell.borrow_mut();
                let luna = borrow.as_mut().or_bug("LUNA not set")?;
                while let Some(packet) = luna.next() {
                    decoder.handle_raw_packet(&mut cap, &packet?)?;
                }
                Ok(())
            })?;

            drop(cap);

            MODELS.with::<_, Result<(), PacketryError>>(|models| {
                for model in models.borrow().iter() {
                    if let Ok(traffic_model) = model
                        .clone()
                        .downcast::<TrafficModel>()
                    {
                        traffic_model.update()?;
                    } else if let Ok(device_model) = model
                        .clone()
                        .downcast::<DeviceModel>()
                    {
                        device_model.update()?;
                    };
                }
                Ok(())
            })?;
            Ok(())
        };

        gtk::glib::timeout_add_local(
            std::time::Duration::from_millis(1),
            move || {
                let result = update_routine();
                if result.is_ok() {
                    Continue(true)
                } else {
                    display_error(result);
                    Continue(false)
                }
            }
        );
    }

    let listview = create_view::<capture::TrafficItem,
                                 model::TrafficModel,
                                 row_data::TrafficRowData>(
                                     &app_capture,
                                     #[cfg(feature="record-ui-test")]
                                     create_logfile(&test_dir, "traffic"));

    let scrolled_window = gtk::ScrolledWindow::builder()
        .hscrollbar_policy(gtk::PolicyType::Automatic)
        .min_content_height(480)
        .min_content_width(640)
        .build();

    scrolled_window.set_child(Some(&listview));

    let device_tree = create_view::<capture::DeviceItem,
                                    model::DeviceModel,
                                    row_data::DeviceRowData>(
                                        &app_capture,
                                        #[cfg(feature="record-ui-test")]
                                        create_logfile(&test_dir, "devices"));

    let device_window = gtk::ScrolledWindow::builder()
        .hscrollbar_policy(gtk::PolicyType::Automatic)
        .min_content_height(480)
        .min_content_width(240)
        .child(&device_tree)
        .build();

    let paned = gtk::Paned::builder()
        .orientation(Orientation::Horizontal)
        .wide_handle(true)
        .start_child(&scrolled_window)
        .end_child(&device_window)
        .build();

    window.set_child(Some(&paned));

    Ok(())
}

fn display_error(result: Result<(), PacketryError>) {
    if let Err(e) = result {
        let message = format!("{}", e);
        WINDOW.with(|win_opt| {
            match win_opt.borrow().as_ref() {
                None => println!("{}", message),
                Some(window) => {
                    let dialog = MessageDialog::new(
                        Some(window),
                        DialogFlags::MODAL,
                        MessageType::Error,
                        ButtonsType::Close,
                        &message
                    );
                    dialog.set_transient_for(Some(window));
                    dialog.set_modal(true);
                    dialog.connect_response(move |dialog, _| dialog.destroy());
                    dialog.show();
                }
            }
        });
    }
}

trait OrBug<T> {
    fn or_bug(self, msg: &'static str) -> Result<T, PacketryError>;
}

impl<T> OrBug<T> for Option<T> {
    fn or_bug(self, msg: &'static str) -> Result<T, PacketryError> {
        self.ok_or(PacketryError::Bug(msg))
    }
}

impl<T, E> OrBug<T> for Result<T, E> {
    fn or_bug(self, msg: &'static str) -> Result<T, PacketryError> {
        self.or(Err(PacketryError::Bug(msg)))
    }
}

fn main() {
    let application = gtk::Application::new(
        Some("com.greatscottgadgets.packetry"),
        Default::default(),
    );
    application.connect_activate(|app| display_error(activate(app)));
    application.run_with_args::<&str>(&[]);
    display_error(
        LUNA.with(|cell|
            if let Some(luna) = cell.take() {
                luna.stop().map_err(PacketryError::Luna)
            } else {
                Ok(())
            }
        )
    );
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::{Arc, Mutex};

    use pcap_file::pcap::PcapReader;

    use crate::capture::{Capture, TrafficItem, DeviceItem};
    use crate::decoder::Decoder;
    use crate::model::{GenericModel, TrafficModel, DeviceModel};
    use crate::record_ui::check_replay;
    use crate::row_data::{TrafficRowData, DeviceRowData};

    #[test]
    fn test_replay() {
        let test_dir = "./tests/ui/";
        for result in std::fs::read_dir(test_dir).unwrap() {
            let entry = result.unwrap();
            if entry.file_type().unwrap().is_dir() {
                let path = entry.path();
                let mut cap_path = path.clone();
                let mut traffic_path = path.clone();
                let mut devices_path = path.clone();
                cap_path.push("capture.pcap");
                traffic_path.push("traffic-log.json");
                devices_path.push("devices-log.json");
                let pcap_file = File::open(cap_path).unwrap();
                let pcap_reader = PcapReader::new(pcap_file).unwrap();
                let mut cap = Capture::new().unwrap();
                let mut decoder = Decoder::new(&mut cap).unwrap();
                for result in pcap_reader {
                    let packet = result.unwrap().data;
                    decoder.handle_raw_packet(&mut cap, &packet).unwrap();
                }
                drop(decoder);
                let capture = Arc::new(Mutex::new(cap));
                let traffic_model =
                    TrafficModel::new(capture.clone()).unwrap();
                let devices_model =
                    DeviceModel::new(capture.clone()).unwrap();
                check_replay::<TrafficModel, TrafficRowData, TrafficItem>(
                    &traffic_model, &traffic_path);
                check_replay::<DeviceModel, DeviceRowData, DeviceItem>(
                    &devices_model, &devices_path);
            }
        }
    }
}
