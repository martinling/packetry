#[macro_use]
extern crate bitfield;
use thiserror::Error;

mod backend;
mod model;
mod row_data;
mod expander;
mod tree_list_model;

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use gtk::gio::ListModel;
use gtk::glib::{self, Object};
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

use model::{GenericModel, TrafficModel};
use row_data::GenericRowData;
use expander::ExpanderWrapper;
use tree_list_model::ModelError;
use backend::luna;

mod capture;
use capture::{Capture, CaptureError, ItemSource};

mod decoder;
use decoder::Decoder;

mod id;
mod file_vec;
mod hybrid_index;
mod usb;
mod vec_map;

thread_local!(
    static MODELS: RefCell<Vec<Object>>  = RefCell::new(Vec::new());
    static LUNA: RefCell<Option<backend::luna::LunaCapture>> = RefCell::new(None);
    static WINDOW: RefCell<Option<ApplicationWindow>> = RefCell::new(None);
);

fn create_view<Item: 'static, Model, RowData>(capture: &Arc<Mutex<Capture>>)
        -> ListView
    where
        Item: Copy,
        Model: GenericModel<Item> + IsA<ListModel> + IsA<Object>,
        RowData: GenericRowData<Item> + IsA<Object>,
        Capture: ItemSource<Item>
{
    let model = Model::new(capture.clone())
                      .expect("Failed to create model");

    MODELS.with(|models| models.borrow_mut().push(model.clone().upcast()));
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

        let node_ref = row.node();
        let node = node_ref.borrow();
        let summary = node.field(&cap_arc, Box::new(Capture::summary));
        expander_wrapper.set_text(summary);
        let connectors = node.field(&cap_arc, Box::new(Capture::connectors));
        expander_wrapper.set_connectors(connectors);
        let expander = expander_wrapper.expander();
        expander.set_visible(node.expandable());
        expander.set_expanded(node.expanded());
        let model = model.clone();
        let node_ref = node_ref.clone();
        let handler = expander.connect_expanded_notify(move |expander| {
            if let Err(e) =
                model.set_expanded(&node_ref, expander.is_expanded())
            {
                display_error(&PacketryError::Model(e));
            }
        });
        expander_wrapper.set_handler(handler);
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
    factory.connect_bind(move |_, list_item| {
        if let Err(e) = bind(list_item) { display_error(&e); }
    });
    factory.connect_unbind(move |_, list_item| {
        if let Err(e) = unbind(list_item) { display_error(&e); }
    });
    ListView::new(Some(&selection_model), Some(&factory))
}

#[derive(Error, Debug)]
pub enum PacketryError {
    #[error("capture data error: {0}")]
    Capture(#[from] CaptureError),
    #[error("tree model error: {0}")]
    Model(#[from] ModelError),
    #[error("pcap error: {0}")]
    Pcap(#[from] pcap::Error),
    #[error("LUNA error: {0}")]
    Luna(#[from] luna::Error),
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
    let mut cap = Capture::new()?;
    let mut decoder = Decoder::new(&mut cap)?;
    let capture = Arc::new(Mutex::new(cap));
    let app_capture = capture.clone();

    if args.len() > 1 {
        let mut pcap = pcap::Capture::from_file(&args[1])?;
        let mut cap = capture.lock().or(Err(PacketryError::Lock))?;
        while let Ok(packet) = pcap.next() {
            decoder.handle_raw_packet(&mut cap, &packet)?;
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
                    let model = model.clone();
                    if let Ok(tree_model) = model.downcast::<TrafficModel>() {
                        tree_model.update()?;
                    };
                }
                Ok(())
            })?;
            Ok(())
        };

        gtk::glib::timeout_add_local(
            std::time::Duration::from_millis(1),
            move || {
                match update_routine() {
                    Ok(_) => Continue(true),
                    Err(e) => {
                        display_error(&e);
                        Continue(false)
                    }
                }
            }
        );
    }

    let listview = create_view::<capture::TrafficItem,
                                 model::TrafficModel,
                                 row_data::TrafficRowData>(&app_capture);

    let scrolled_window = gtk::ScrolledWindow::builder()
        .hscrollbar_policy(gtk::PolicyType::Automatic)
        .min_content_height(480)
        .min_content_width(640)
        .build();

    scrolled_window.set_child(Some(&listview));

    let device_tree = create_view::<capture::DeviceItem,
                                    model::DeviceModel,
                                    row_data::DeviceRowData>(&app_capture);
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

fn display_error(e: &PacketryError) {
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
                dialog.connect_response(
                    glib::clone!(@weak window => move |_, _| window.destroy()));
                dialog.show();
            }
        }
    });
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
    application.connect_activate(|application| {
        if let Err(e) = activate(application) { display_error(&e) }
    });
    application.run_with_args::<&str>(&[]);
    let stop_result = LUNA.with(|cell| {
        if let Some(luna) = cell.take() {
            luna.stop()
        } else {
            Ok(())
        }
    });
    if let Err(e) = stop_result {
        display_error(&PacketryError::Luna(e));
    }
}
