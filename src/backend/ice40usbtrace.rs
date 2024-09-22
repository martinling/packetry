use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{Context, Error};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use nusb::{
    self,
    transfer::{Control, ControlType, Recipient},
    DeviceInfo, Interface,
};

use crate::usb::crc5;

use super::{
    CaptureDevice,
    CaptureHandle,
    Speed,
    TimestampedPacket,
    TransferQueue,
};

pub const VID_PID: (u16, u16) = (0x1d50, 0x617e);
const INTERFACE: u8 = 1;
const ENDPOINT: u8 = 0x81;
const READ_LEN: usize = 1024;
const NUM_TRANSFERS: usize = 4;
const SPEEDS: [Speed; 1] = [Speed::Full];

#[derive(Debug, Clone, Copy, IntoPrimitive)]
#[repr(u8)]
enum Command {
    //CaptureStatus = 0x10,
    CaptureStart = 0x12,
    CaptureStop = 0x13,
    //BufferGetLevel = 0x20,
    BufferFlush = 0x21,
}

/// A iCE40-usbtrace device attached to the system.
pub struct Ice40UsbtraceDevice {
    pub device_info: DeviceInfo,
}

/// A handle to an open iCE40-usbtrace device.
#[derive(Clone)]
pub struct Ice40UsbtraceHandle {
    interface: Interface,
}

/// Converts from received data bytes to timestamped packets.
pub struct Ice40UsbtraceStream {
    receiver: mpsc::Receiver<Vec<u8>>,
    buffer: VecDeque<u8>,
    ts: u64,
}

/// Probe an iCE40-usbtrace device.
pub fn probe(device_info: DeviceInfo) -> Result<Box<dyn CaptureDevice>, Error> {
    // Check we can open the device.
    let device = device_info
        .open()
        .context("Failed to open device")?;

    // Read the active configuration.
    let _config = device
        .active_configuration()
        .context("Failed to retrieve active configuration")?;

    // Try to claim the interface.
    let _interface = device
        .claim_interface(INTERFACE)
        .context("Failed to claim interface")?;

    // Now we have a usable device.
    Ok(Box::new(Ice40UsbtraceDevice { device_info }))
}

impl CaptureDevice for Ice40UsbtraceDevice {
    fn open_as_generic(&self) -> Result<Box<dyn CaptureHandle>, Error> {
        let device = self.device_info.open()?;
        let interface = device.claim_interface(INTERFACE)?;
        Ok(Box::new(Ice40UsbtraceHandle { interface }))
    }

    fn supported_speeds(&self) -> &[Speed] {
        &SPEEDS
    }
}

impl CaptureHandle for Ice40UsbtraceHandle {

    fn begin_capture(
        &mut self,
        speed: Speed,
        data_tx: mpsc::Sender<Vec<u8>>
    ) -> Result<TransferQueue, Error> {
        // iCE40-usbtrace only supports full-speed captures
        assert_eq!(speed, Speed::Full);

        // Stop the device if it was left running before and ignore any errors
        let _ = self.stop_capture();
        sleep(Duration::from_millis(100));
        let _ = self.flush_buffer();

        // Start capture.
        self.start_capture()?;

        // Set up transfer queue.
        Ok(TransferQueue::new(&self.interface, data_tx,
            ENDPOINT, NUM_TRANSFERS, READ_LEN))
    }

    fn end_capture(&mut self) -> Result<(), Error> {
        // Stop the capture.
        self.stop_capture()?;

        // Leave running briefly to receive flushed data.
        sleep(Duration::from_millis(100));

        Ok(())
    }

    fn post_capture(&mut self) -> Result<(), Error> {
        self.flush_buffer()
    }

    fn timestamped_packets(&self, data_rx: mpsc::Receiver<Vec<u8>>)
        -> Box<dyn Iterator<Item=TimestampedPacket> + Send> {
        Box::new(
            Ice40UsbtraceStream {
                receiver: data_rx,
                buffer: VecDeque::new(),
                ts: 0,
            }
        )
    }

    fn duplicate(&self) -> Box<dyn CaptureHandle> {
        Box::new(self.clone())
    }
}

impl Ice40UsbtraceHandle {

    fn start_capture(&mut self) -> Result<(), Error> {
        self.write_request(Command::CaptureStart)
    }

    fn stop_capture(&mut self) -> Result<(), Error> {
        self.write_request(Command::CaptureStop)
    }

    fn flush_buffer(&mut self) -> Result<(), Error> {
        self.write_request(Command::BufferFlush)
    }

    fn write_request(&mut self, request: Command) -> Result<(), Error> {
        let control = Control {
            control_type: ControlType::Vendor,
            recipient: Recipient::Interface,
            request: request.into(),
            value: 0,
            index: 0,
        };
        let data = &[];
        let timeout = Duration::from_secs(1);
        self.interface
            .control_out_blocking(control, data, timeout)
            .context("Write request failed")?;
        Ok(())
    }
}

impl Iterator for Ice40UsbtraceStream {
    type Item = TimestampedPacket;

    fn next(&mut self) -> Option<TimestampedPacket> {
        loop {
            // Do we have another packet already in the buffer?
            match self.next_buffered_packet() {
                // Yes; return the packet.
                Some(packet) => return Some(packet),
                // No; wait for more data from the capture thread.
                None => match self.receiver.recv().ok() {
                    // Received more data; add it to the buffer and retry.
                    Some(bytes) => self.buffer.extend(bytes.iter()),
                    // Capture has ended, there are no more packets.
                    None => return None,
                },
            }
        }
    }
}

bitfield! {
    pub struct Header(MSB0 [u8]);
    impl Debug;
    u16;
    // 24MHz ticks
    pub ts, _: 31, 16;
    pub u8, pid, _: 3, 0;
    pub ok, _: 4;
    pub dat, _: 15, 5;
}

impl<T: std::convert::AsRef<[u8]>> Header<T> {
    pub fn pid_byte(&self) -> u8 {
        let pid = self.pid();

        pid | ((pid ^ 0xf) << 4)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum Pid {
    Out = 0b0001,
    In = 0b1001,
    Sof = 0b0101,
    Setup = 0b1101,

    Data0 = 0b0011,
    Data1 = 0b1011,

    Ack = 0b0010,
    Nak = 0b1010,
    Stall = 0b1110,
    TsOverflow = 0b0000,
}

impl std::fmt::Display for Pid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Pid::Out => "OUT",
            Pid::In => "IN",
            Pid::Sof => "SOF",
            Pid::Setup => "SETUP",
            Pid::Data0 => "DATA0",
            Pid::Data1 => "DATA1",
            Pid::Ack => "ACK",
            Pid::Nak => "NAK",
            Pid::Stall => "STALL",
            Pid::TsOverflow => "TS OVERFLOW",
        };
        write!(f, "{}", s)
    }
}

impl Ice40UsbtraceStream {
    fn ns(&self) -> u64 {
        // 24MHz clock, a tick is 41.666...ns
        const TABLE: [u64; 3] = [0, 41, 83];
        let quotient = self.ts / 3;
        let remainder = self.ts % 3;
        quotient * 125 + TABLE[remainder as usize]
    }

    fn parse_packet(&mut self) -> ParseResult<TimestampedPacket> {
        let header: Vec<u8> = self.buffer.drain(0..4).collect();
        let header = Header(&header);

        self.ts += u64::from(header.ts());

        let pkt = match (header.pid().try_into(), header.ok()) {
            // The packet header could not even be decoded, skip it
            (Ok(Pid::TsOverflow), false) => {
                println!("Bad packet!\n{header:?}");
                return ParseResult::Ignored;
            }
            // Need to increment self.ts
            (Ok(Pid::TsOverflow), true) => ParseResult::Ignored,
            // Handle Data packet. If the CRC16 is wrong get_ok() returns false - push broken packet regardless
            (Ok(Pid::Data0 | Pid::Data1), data_ok) => {
                if !data_ok {
                    println!("Data packet with corrupt checksum:\n{header:?}");
                }

                let mut bytes = vec![header.pid_byte()];
                let data_len: usize = header.dat().into();
                if self.buffer.len() < data_len {
                    for byte in header.0.iter().rev() {
                        self.buffer.push_front(*byte);
                    }
                    return ParseResult::NeedMoreData;
                }
                bytes.extend(self.buffer.drain(0..data_len));
                ParseResult::Parsed(TimestampedPacket {
                    timestamp_ns: self.ns(),
                    bytes,
                })
            }
            (Ok(Pid::Sof | Pid::Setup | Pid::In | Pid::Out), data_ok) => {
                let mut bytes = vec![header.pid_byte()];
                let mut data = header.dat().to_le_bytes();
                let crc = crc5(u32::from_le_bytes([data[0], data[1], 0, 0]), 11);
                if data_ok {
                    data[1] |= crc << 3;
                } else {
                    println!("PID pattern correct, but broken CRC5:\n{header:?}");
                    data[1] |= (!crc) << 3;
                }
                bytes.extend(data);

                ParseResult::Parsed(TimestampedPacket {
                    timestamp_ns: self.ns(),
                    bytes,
                })
            }
            (Ok(Pid::Ack | Pid::Nak | Pid::Stall), data_ok) => {
                assert!(data_ok, "PID is all there is to decode!");
                let bytes = vec![header.pid_byte()];
                ParseResult::Parsed(TimestampedPacket {
                    timestamp_ns: self.ns(),
                    bytes,
                })
            }
            (Err(_), _) => {
                println!("Error decoding PID for header:\n{header:?}");
                ParseResult::Ignored
            }
        };

        pkt
    }

    fn next_buffered_packet(&mut self) -> Option<TimestampedPacket> {
        loop {
            // Need more bytes for the header
            if self.buffer.len() < 4 {
                return None;
            }

            match self.parse_packet() {
                ParseResult::Parsed(pkt) => return Some(pkt),
                ParseResult::Ignored => continue,
                ParseResult::NeedMoreData => return None,
            }
        }
    }
}

pub enum ParseResult<T> {
    Parsed(T),
    Ignored,
    NeedMoreData,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header() {
        let data: [u8; 4] = [8, 1, 255, 241];
        let header = Header(&data);

        assert_eq!(header.ts(), 65521);
        assert_eq!(header.pid(), 0);
        assert!(header.ok());
        assert_eq!(header.dat(), 1);
    }
}
