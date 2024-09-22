use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread::{JoinHandle, spawn};

use anyhow::{Context, Error, bail};
use futures_channel::oneshot;
use futures_lite::future::block_on;
use futures_util::future::FusedFuture;
use futures_util::{select_biased, FutureExt};
use nusb::{
    self,
    transfer::{Queue, RequestBuffer, TransferError},
    DeviceInfo,
    Interface
};
use num_enum::{FromPrimitive, IntoPrimitive};
use once_cell::sync::Lazy;

use crate::util::handle_thread_panic;

pub mod cynthion;
pub mod ice40usbtrace;

type VidPid = (u16, u16);
type ProbeFn = fn(DeviceInfo) -> Result<Box<dyn CaptureDevice>, Error>;

/// The result of identifying and probing a supported USB device.
pub struct ProbeResult {
    pub name: &'static str,
    pub info: DeviceInfo,
    pub result: Result<Box<dyn CaptureDevice>, String>,
}

/// Map of supported (VID, PID) pairs to device-specific probe functions.
static SUPPORTED_DEVICES: Lazy<BTreeMap<VidPid, (&str, ProbeFn)>> = Lazy::new(||
    BTreeMap::from_iter([
        (cynthion::VID_PID,
            ("Cynthion", cynthion::probe as ProbeFn)),
        (ice40usbtrace::VID_PID,
            ("iCE40-usbtrace", ice40usbtrace::probe as ProbeFn)),
    ])
);

/// Scan for supported devices.
pub fn scan() -> Result<Vec<ProbeResult>, Error> {
    Ok(nusb::list_devices()?
        .filter_map(|info|
            SUPPORTED_DEVICES
                .get(&(info.vendor_id(), info.product_id()))
                .map(|(name, probe)| (name, probe(info.clone())))
                .map(|(name, result)|
                    ProbeResult {
                        name,
                        info,
                        result: result.map_err(|e| format!("{e}"))
                    }
                ))
        .collect()
    )
}

/// A capture device connected to the system, not currently opened.
pub trait CaptureDevice {
    /// Open this device to use it as a generic capture device.
    fn open_as_generic(&self) -> Result<Box<dyn CaptureHandle>, Error>;

    /// Which speeds this device supports.
    fn supported_speeds(&self) -> &[Speed];
}

/// Possible capture speed settings.
#[derive(Debug, Copy, Clone, PartialEq, FromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum Speed {
    #[default]
    High = 0,
    Full = 1,
    Low  = 2,
    Auto = 3,
}

/// A timestamped packet.
pub struct TimestampedPacket {
    pub timestamp_ns: u64,
    pub bytes: Vec<u8>,
}

/// Handle used to stop an ongoing capture.
pub struct CaptureStop {
    stop_tx: oneshot::Sender<()>,
    worker: JoinHandle::<()>,
}

/// A handle to an open capture device.
pub trait CaptureHandle: Send + Sync {

    /// Begin capture.
    ///
    /// This method should send whatever control requests etc are necessary to
    /// start capture, then set up and return a `TransferQueue` that sends the
    /// raw data from the device to `data_tx`.
    fn begin_capture(
        &mut self,
        speed: Speed,
        data_tx: mpsc::Sender<Vec<u8>>)
    -> Result<TransferQueue, Error>;

    /// End capture.
    ///
    /// This method should send whatever control requests etc are necessary to
    /// stop the capture. Once complete the transfer queue will be shut down.
    fn end_capture(&mut self) -> Result<(), Error>;

    /// Post-capture cleanup.
    ///
    /// This method will be called after the transfer queue has been shut down,
    /// and should do any cleanup necessary before next use.
    fn post_capture(&mut self) -> Result<(), Error>;

    /// Construct an iterator that produces timestamped packets from raw data.
    ///
    /// This method must construct a suitable iterator type around `data_rx`,
    /// which will parse the raw data from the device to produce timestamped
    /// packets. The iterator type must be `Send` so that it can be passed to
    /// a separate decoder thread.
    ///
    fn timestamped_packets(&self, data_rx: mpsc::Receiver<Vec<u8>>)
        -> Box<dyn Iterator<Item=TimestampedPacket> + Send>;

    /// Duplicate this handle with Box::new(self.clone())
    ///
    /// The device handle must be cloneable, so that one worker thread can
    /// process the data transfer queue asynchronously, whilst another thread
    /// does control transfers using synchronous calls.
    ///
    /// However, it turns out we cannot actually make `Clone` a prerequisite
    /// of `CaptureHandle`, because doing so prevents the trait from being
    /// object safe. This method provides a workaround.
    fn duplicate(&self) -> Box<dyn CaptureHandle>;

    /// Start capturing in the background.
    ///
    /// The `result_handler` callback will be invoked later from a worker
    /// thread, once the capture is either stopped normally or terminates with
    /// an error.
    ///
    /// Returns:
    /// - an iterator over timestamped packets
    /// - a handle to stop the capture
    fn start(
        &self,
        speed: Speed,
        result_handler: Box<dyn FnOnce(Result<(), Error>) + Send + Sync>
    ) -> Result<(Box<dyn Iterator<Item=TimestampedPacket> + Send>, CaptureStop), Error> {
        // Channel to pass captured data to the decoder thread.
        let (data_tx, data_rx) = mpsc::channel();
        // Channel to stop the capture thread on request.
        let (stop_tx, stop_rx) = oneshot::channel();
        // Duplicate this handle to pass to the worker thread.
        let mut handle = self.duplicate();
        // Start worker thread to run the capture.
        let worker = spawn(move || result_handler(
            handle.run_capture(speed, data_tx, stop_rx)
        ));
        // Iterator over timestamped packets.
        let packets = self.timestamped_packets(data_rx);
        // Handle to stop the worker thread.
        let stop_handle = CaptureStop { worker, stop_tx };
        Ok((packets, stop_handle))
    }

    /// Worker that runs the whole lifecycle of a capture from start to finish.
    fn run_capture(
        &mut self,
        speed: Speed,
        data_tx: mpsc::Sender<Vec<u8>>,
        stop_rx: oneshot::Receiver<()>,
    ) -> Result<(), Error> {
        // Set up a separate channel pair to stop queue processing.
        let (queue_stop_tx, queue_stop_rx) = oneshot::channel();

        // Begin capture and set up transfer queue.
        let mut transfer_queue = self.begin_capture(speed, data_tx)?;
        eprintln!("Capture enabled, speed: {}", speed.description());

        // Spawn a worker thread to process the transfer queue until stopped.
        let queue_worker = spawn(move ||
            block_on(transfer_queue.process(queue_stop_rx))
        );

        // Wait until this thread is signalled to stop.
        block_on(stop_rx)
            .context("Sender was dropped")?;

        // End capture.
        self.end_capture()?;
        eprintln!("Capture disabled");

        // Signal queue processing to stop, then join the worker thread.
        queue_stop_tx.send(())
            .or_else(|_| bail!("Failed sending stop signal to queue worker"))?;

        handle_thread_panic(queue_worker.join())?
            .context("Error in queue worker thread")?;

        // Run any post-capture cleanup required by the device.
        self.post_capture()?;

        Ok(())
    }
}

/// A queue of USB transfers, feeding data blocks to the decoder thread.
pub struct TransferQueue {
    queue: Queue<RequestBuffer>,
    data_tx: mpsc::Sender<Vec<u8>>,
    transfer_length: usize,
}

impl TransferQueue {
    /// Create a new transfer queue.
    fn new(
        interface: &Interface,
        data_tx: mpsc::Sender<Vec<u8>>,
        endpoint: u8,
        num_transfers: usize,
        transfer_length: usize
    ) -> TransferQueue {
        let mut queue = interface.bulk_in_queue(endpoint);
        while queue.pending() < num_transfers {
            queue.submit(RequestBuffer::new(transfer_length));
        }
        TransferQueue { queue, data_tx, transfer_length }
    }

    /// Process the queue, sending data to the decoder thread until stopped.
    async fn process(&mut self, mut stop_rx: oneshot::Receiver<()>)
        -> Result<(), Error>
    {
        use TransferError::Cancelled;
        loop {
            select_biased!(
                _ = stop_rx => {
                    // Stop requested. Cancel all transfers.
                    self.queue.cancel_all();
                }
                completion = self.queue.next_complete().fuse() => {
                    match completion.status {
                        Ok(()) => {
                            // Send data to decoder thread.
                            self.data_tx.send(completion.data)
                                .context(
                                    "Failed sending capture data to channel")?;
                            if !stop_rx.is_terminated() {
                                // Submit next transfer.
                                self.queue.submit(
                                    RequestBuffer::new(self.transfer_length)
                                );
                            }
                        },
                        Err(Cancelled) if stop_rx.is_terminated() => {
                            // Transfer cancelled during shutdown. Drop it.
                            drop(completion);
                            if self.queue.pending() == 0 {
                                // All cancellations now handled.
                                return Ok(());
                            }
                        },
                        Err(usb_error) => {
                            // Transfer failed.
                            return Err(Error::from(usb_error));
                        }
                    }
                }
            );
        }
    }
}

impl CaptureStop {
    /// Stop the capture associated with this handle.
    pub fn stop(self) -> Result<(), Error> {
        eprintln!("Requesting capture stop");
        self.stop_tx.send(())
            .or_else(|_| bail!("Failed sending stop request"))?;
        handle_thread_panic(self.worker.join())?;
        Ok(())
    }
}
