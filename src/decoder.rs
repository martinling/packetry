use std::mem::size_of;

use crate::usb::{self, prelude::*};
use crate::capture::{prelude::*, INVALID_EP_NUM, FRAMING_EP_NUM};
use crate::hybrid_index::{HybridIndex, Number};
use crate::vec_map::{VecMap, Key};

use CaptureError::IndexError;

impl PID {
    fn from_packet(packet: &[u8]) -> Result<PID, CaptureError> {
        let first_byte = packet
            .first()
            .ok_or_else(||
                IndexError(String::from(
                    "Packet is empty, cannot retrieve PID")))?;
        Ok(PID::from(*first_byte))
    }
}

#[derive(PartialEq, Eq)]
enum DecodeStatus {
    Single,
    New,
    Continue,
    Retry,
    Done,
    Invalid
}

struct EndpointData {
    device_id: DeviceId,
    address: EndpointAddr,
    start_item: Option<TrafficItemId>,
    active: Option<EndpointTransferId>,
    ended: Option<EndpointTransferId>,
    transaction_count: u64,
    last: Option<PID>,
    last_success: bool,
    setup: Option<SetupFields>,
    payload: Vec<u8>,
}

#[derive(Default)]
struct TransactionState {
    first: Option<PID>,
    last: Option<PID>,
    start: Option<PacketId>,
    split_sc: Option<StartComplete>,
    split_pid: Option<PID>,
    count: u64,
    endpoint_id: Option<EndpointId>,
    setup: Option<SetupFields>,
    payload: Vec<u8>,
}

impl TransactionState {
    pub fn status(&mut self, packet: &[u8])
        -> Result<DecodeStatus, CaptureError>
    {
        let next = PID::from_packet(packet)?;
        use PID::*;
        use DecodeStatus::*;
        Ok(match (self.first, self.last, next) {

            // SPLIT starts a new transaction.
            (_, _, SPLIT) => New,

            // SPLIT must be handled according to start/complete flag.
            (Some(SPLIT), Some(last), _) => {
                let sc = self.split_sc
                    .ok_or_else(|| IndexError(String::from(
                        "SPLIT start/complete flag not set")))?;

                use StartComplete::*;
                match (sc, last, next) {

                    // SPLIT must always be followed by SETUP, IN or OUT.
                    (_, SPLIT, SETUP | IN | OUT) => Continue,

                    // After SSPLIT and SETUP, DATA0 must follow.
                    (Start, SETUP, DATA0) => {
                        match packet.len() {
                            11 => {
                                self.setup = Some(
                                    SetupFields::from_data_packet(packet));
                                // Wait for ACK.
                                Continue
                            },
                            _ => Invalid
                        }
                    },

                    // After SSPLIT and IN, ACK or NAK end transaction.
                    (Start, IN, ACK | NAK) => Done,

                    // After SSPLIT and OUT, DATA0 or DATA1 must follow.
                    (Start, OUT, DATA0 | DATA1) => Continue,

                    // ...then ACK or NAK end transaction.
                    (Start, DATA0 | DATA1, ACK | NAK) => Done,

                    // After CSPLIT and IN, DATA0/DATA1/NAK/STALL/NYET follow.
                    (Complete, IN, DATA0 | DATA1 | NAK | STALL | NYET) => Done,

                    // After CSPLIT and OUT/SETUP, NYET/STALL/ACK/NAK follow.
                    (Complete, OUT | SETUP, NYET | STALL | ACK | NAK) => Done,

                    // Any other combination is invalid.
                    (..) => Invalid,
                }
            },

            // Unless handled above after SPLIT, SETUP/IN/OUT/PING always
            // start a new transaction.
            (_, _, SETUP | IN | OUT | PING) => New,

            // SOF when there is no existing transaction starts a new
            // "transaction" representing an idle period on the bus.
            (_, None, SOF) => New,
            // Additional SOFs extend this "transaction", more may follow.
            (_, Some(SOF), SOF) => Continue,

            // A malformed packet is grouped with previous malformed packets.
            (_, Some(Malformed), Malformed) => Continue,
            // If preceded by any other packet, it starts a new transaction.
            (_, _, Malformed) => New,

            // SETUP must be followed by DATA0.
            (_, Some(SETUP), DATA0) => {
                // The packet must have the correct size.
                match packet.len() {
                    11 => {
                        self.setup = Some(
                            SetupFields::from_data_packet(packet));
                        // Wait for ACK.
                        Continue
                    },
                    _ => Invalid
                }
            }
            // ACK then completes the transaction.
            (Some(SETUP), Some(DATA0), ACK) => Done,

            // IN may be followed by NAK or STALL, completing transaction.
            (_, Some(IN), NAK | STALL) => Done,
            // IN or OUT may be followed by DATA0 or DATA1, wait for status.
            (_, Some(IN | OUT), DATA0 | DATA1) => {
                if packet.len() >= 3 {
                    let range = 1 .. (packet.len() - 2);
                    self.payload = packet[range].to_vec();
                    Continue
                } else {
                    Invalid
                }
            },
            // An ACK or NYET then completes the transaction.
            (Some(IN | OUT), Some(DATA0 | DATA1), ACK | NYET) => Done,
            // OUT may also be completed by NAK or STALL.
            (Some(OUT), Some(DATA0 | DATA1), NAK | STALL) => Done,

            // PING may be followed by ACK or NAK.
            (Some(PING), Some(PING), ACK | NAK) => Done,

            // Any other case is not a valid part of a transaction.
            _ => Invalid,
        })
    }

    fn successful(&self) -> bool {
        use PID::*;
        use StartComplete::*;
        match (self.first, self.last) {

            // SPLIT success depends on start/complete flag.
            (Some(SPLIT), Some(last)) => match (self.split_sc, last) {

                // SSPLIT is successful if it ends with ACK/NYET.
                (Some(Start), ACK | NYET) => true,

                // CSPLIT is successful if it ends with DATA0/DATA1/ACK/NYET.
                (Some(Complete), DATA0 | DATA1 | ACK | NYET) => true,

                (..) => false,
            },

            // PING is successful if it ends with ACK.
            (Some(PING), Some(ACK)) => true,

            // SETUP/IN/OUT is successful if it ends with ACK/NYET.
            (Some(SETUP | IN | OUT), Some(ACK | NYET)) => true,

            (..) => false
        }
    }
}

#[derive(Copy, Clone)]
struct EndpointKey {
    dev_addr: DeviceAddr,
    direction: Direction,
    ep_num: EndpointNum,
}

impl Key for EndpointKey {
    fn id(self) -> usize {
        self.dev_addr.0 as usize * 32 +
            self.direction as usize * 16 +
                self.ep_num.0 as usize
    }

    fn key(id: usize) -> EndpointKey {
        EndpointKey {
            dev_addr: DeviceAddr((id / 32) as u8),
            direction: Direction::from(((id / 16) % 2) as u8),
            ep_num: EndpointNum((id % 16) as u8),
        }
    }
}

pub struct Decoder {
    device_index: VecMap<DeviceAddr, DeviceId>,
    endpoint_index: VecMap<EndpointKey, EndpointId>,
    endpoint_data: VecMap<EndpointId, EndpointData>,
    last_endpoint_state: Vec<u8>,
    last_item_endpoint: Option<EndpointId>,
    transaction_state: TransactionState,
}

impl Decoder {
    pub fn new(capture: &mut Capture) -> Result<Self, CaptureError> {
        let mut decoder = Decoder {
            device_index: VecMap::new(),
            endpoint_index: VecMap::new(),
            endpoint_data: VecMap::new(),
            last_endpoint_state: Vec::new(),
            last_item_endpoint: None,
            transaction_state: TransactionState::default(),
        };
        let invalid_id = decoder.add_endpoint(
            capture, DeviceAddr(0), EndpointNum(INVALID_EP_NUM), Direction::Out)?;
        let framing_id = decoder.add_endpoint(
            capture, DeviceAddr(0), EndpointNum(FRAMING_EP_NUM), Direction::Out)?;
        assert!(invalid_id == Decoder::INVALID_EP_ID);
        assert!(framing_id == Decoder::FRAMING_EP_ID);
        Ok(decoder)
    }

    const INVALID_EP_ID: EndpointId = EndpointId::constant(0);
    const FRAMING_EP_ID: EndpointId = EndpointId::constant(1);

    pub fn handle_raw_packet(&mut self, capture: &mut Capture, packet: &[u8])
        -> Result<(), CaptureError>
    {
        let data_id = capture.packet_data.append(packet)?;
        let packet_id = capture.packet_index.push(data_id)?;
        self.transaction_update(capture, packet_id, packet)?;
        Ok(())
    }

    pub fn token_endpoint(&mut self, capture: &mut Capture, pid: PID, token: &TokenFields)
        -> Result<EndpointId, CaptureError>
    {
        let dev_addr = token.device_address();
        let ep_num = token.endpoint_number();
        let direction = match (ep_num.0, pid) {
            (0, _)         => Direction::Out,
            (_, PID::IN)   => Direction::In,
            (_, PID::OUT)  => Direction::Out,
            (_, PID::PING) => Direction::Out,
            _ => return Err(IndexError(format!(
                "PID {} does not indicate a direction", pid)))
        };
        let key = EndpointKey {
            dev_addr,
            ep_num,
            direction
        };
        Ok(match self.endpoint_index.get(key) {
            Some(id) => *id,
            None => {
                let id = self.add_endpoint(
                    capture, key.dev_addr, key.ep_num, key.direction)?;
                self.endpoint_index.set(key, id);
                id
            }
        })
    }

    fn packet_endpoint(&mut self, capture: &mut Capture, packet: &[u8])
        -> Result<EndpointId, CaptureError>
    {
        let pid = PID::from_packet(packet)?;
        Ok(match PacketFields::from_packet(packet) {
            PacketFields::SOF(_) => Decoder::FRAMING_EP_ID,
            PacketFields::Token(token) =>
                self.token_endpoint(capture, pid, &token)?,
            _ => Decoder::INVALID_EP_ID,
        })
    }

    fn transaction_update(&mut self, capture: &mut Capture, packet_id: PacketId, packet: &[u8])
        -> Result<(), CaptureError>
    {
        use DecodeStatus::*;
        match self.transaction_state.status(packet)? {
            Single => {
                self.transaction_start(capture, packet_id, packet)?;
                self.transaction_end(capture)?;
            },
            New => {
                self.transaction_start(capture, packet_id, packet)?;
            },
            Continue | Retry => {
                self.transaction_append(capture, packet)?;
            },
            Done => {
                self.transaction_append(capture, packet)?;
                self.transaction_end(capture)?;
            },
            Invalid => {
                self.transaction_start(capture, packet_id, packet)?;
                self.transaction_end(capture)?;
            },
        };
        Ok(())
    }

    fn transaction_start(&mut self, capture: &mut Capture, packet_id: PacketId, packet: &[u8])
        -> Result<(), CaptureError>
    {
        self.add_transaction(capture)?;
        self.transaction_state = TransactionState::default();
        let pid = PID::from_packet(packet)?;
        let state = &mut self.transaction_state;
        state.start = Some(packet_id);
        state.count = 1;
        state.first = Some(pid);
        state.last = state.first;
        self.transaction_state.endpoint_id =
            Some(self.packet_endpoint(capture, packet)?);
        if pid == PID::SPLIT {
            let split = SplitFields::from_packet(packet);
            self.transaction_state.split_sc = Some(split.sc());
        };
        Ok(())
    }

    fn transaction_append(&mut self, capture: &mut Capture, packet: &[u8])
        -> Result<(), CaptureError>
    {
        let pid = PID::from_packet(packet)?;
        self.transaction_state.count += 1;
        if self.transaction_state.last == Some(PID::SPLIT) {
            self.transaction_state.endpoint_id =
                Some(self.packet_endpoint(capture, packet)?);
            self.transaction_state.split_pid = Some(pid);
        }
        self.transaction_state.last = Some(pid);
        Ok(())
    }

    fn transaction_end(&mut self, capture: &mut Capture)
        -> Result<(), CaptureError>
    {
        self.add_transaction(capture)?;
        self.transaction_state = TransactionState::default();
        Ok(())
    }

    fn add_transaction(&mut self, capture: &mut Capture)
        -> Result<(), CaptureError>
    {
        if self.transaction_state.count == 0 { return Ok(()) }
        let start_packet_id =
            self.transaction_state.start.ok_or_else(||
                IndexError(String::from(
                    "Transaction state has no start PID")))?;
        let transaction_id =
            capture.transaction_index.push(start_packet_id)?;
        self.transfer_update(capture, transaction_id)?;
        Ok(())
    }

    fn add_device(&mut self, capture: &mut Capture, address: DeviceAddr)
        -> Result<DeviceId, CaptureError>
    {
        let device = Device { address };
        let device_id = capture.devices.push(&device)?;
        self.device_index.set(address, device_id);
        capture.device_data.set(device_id, DeviceData {
            device_descriptor: None,
            configurations: VecMap::new(),
            config_number: None,
            endpoint_details: VecMap::new(),
            strings: VecMap::new(),
        });
        Ok(device_id)
    }

    fn add_endpoint(&mut self,
                    capture: &mut Capture,
                    dev_addr: DeviceAddr,
                    number: EndpointNum,
                    direction: Direction)
        -> Result<EndpointId, CaptureError>
    {
        let device_id = match self.device_index.get(dev_addr) {
            Some(id) => *id,
            None => self.add_device(capture, dev_addr)?
        };
        let mut endpoint = Endpoint::default();
        endpoint.set_device_id(device_id);
        endpoint.set_device_address(dev_addr);
        endpoint.set_number(number);
        endpoint.set_direction(direction);
        let endpoint_id = capture.endpoints.push(&endpoint)?;
        let address = EndpointAddr::from_parts(number, direction);
        self.endpoint_data.set(endpoint_id, EndpointData {
            address,
            device_id,
            start_item: None,
            active: None,
            ended: None,
            transaction_count: 0,
            last: None,
            last_success: false,
            setup: None,
            payload: Vec::new(),
        });
        capture.endpoint_traffic.set(endpoint_id, EndpointTraffic {
            transaction_ids: HybridIndex::new(1)?,
            transfer_index: HybridIndex::new(1)?,
            data_index: HybridIndex::new(1)?,
            total_data: 0,
            first_item_id: None,
            progress_index: HybridIndex::new(1)?,
            end_index: HybridIndex::new(1)?,
        });
        let ep_state = EndpointState::Idle as u8;
        self.last_endpoint_state.push(ep_state);
        Ok(endpoint_id)
    }

    fn current_endpoint_id(&self) -> Result<EndpointId, CaptureError> {
        self.transaction_state.endpoint_id.ok_or_else(||
            IndexError(String::from(
                "Transaction state has no endpoint ID set")))
    }

    fn current_endpoint_data(&self) -> Result<&EndpointData, CaptureError> {
        let endpoint_id = self.current_endpoint_id()?;
        self.endpoint_data.get(endpoint_id).ok_or_else(||
            IndexError(format!(
                "Decoder has no data for current endpoint ID {}",
                endpoint_id)))
    }

    fn current_endpoint_data_mut(&mut self)
        -> Result<&mut EndpointData, CaptureError>
    {
        let endpoint_id = self.current_endpoint_id()?;
        self.endpoint_data.get_mut(endpoint_id).ok_or_else(||
            IndexError(format!(
                "Decoder has no data for current endpoint ID {}",
                endpoint_id)))
    }

    fn current_device_data<'a>(&self, capture: &'a mut Capture)
        -> Result<&'a DeviceData, CaptureError>
    {
        let ep_data = self.current_endpoint_data()?;
        capture.device_data(&ep_data.device_id)
    }

    fn current_device_data_mut<'a>(&mut self, capture: &'a mut Capture)
        -> Result<&'a mut DeviceData, CaptureError>
    {
        let ep_data = self.current_endpoint_data()?;
        let device_id = ep_data.device_id;
        capture.device_data_mut(&device_id)
    }

    fn decode_request(&mut self, capture: &mut Capture, fields: SetupFields)
        -> Result<(), CaptureError>
    {
        let req_type = fields.type_fields.request_type();
        let request = StandardRequest::from(fields.request);
        match (req_type, request) {
            (RequestType::Standard, StandardRequest::GetDescriptor)
                => self.decode_descriptor_read(capture, &fields)?,
            (RequestType::Standard, StandardRequest::SetConfiguration)
                => self.decode_configuration_set(capture, &fields)?,
            _ => ()
        }
        Ok(())
    }

    fn decode_descriptor_read(&mut self, capture: &mut Capture, fields: &SetupFields)
        -> Result<(), CaptureError>
    {
        let recipient = fields.type_fields.recipient();
        let desc_type = DescriptorType::from((fields.value >> 8) as u8);
        let payload = &self.current_endpoint_data()?.payload;
        let length = payload.len();
        match (recipient, desc_type) {
            (Recipient::Device, DescriptorType::Device) => {
                if length == size_of::<DeviceDescriptor>() {
                    let descriptor = DeviceDescriptor::from_bytes(payload);
                    let dev_data = self.current_device_data_mut(capture)?;
                    dev_data.device_descriptor = Some(descriptor);
                }
            },
            (Recipient::Device, DescriptorType::Configuration) => {
                let size = size_of::<ConfigDescriptor>();
                if length >= size {
                    let configuration = Configuration::from_bytes(payload);
                    let dev_data = self.current_device_data_mut(capture)?;
                    if let Some(config) = configuration {
                        let configurations = &mut dev_data.configurations;
                        let config_num = ConfigNum::from(
                            config.descriptor.config_value);
                        configurations.set(config_num, config);
                        dev_data.update_endpoint_details();
                    }
                }
            },
            (Recipient::Device, DescriptorType::String) => {
                if length >= 2 {
                    let string = UTF16ByteVec(payload[2..length].to_vec());
                    let dev_data = self.current_device_data_mut(capture)?;
                    let strings = &mut dev_data.strings;
                    let string_id =
                        StringId::from((fields.value & 0xFF) as u8);
                    strings.set(string_id, string);
                }
            },
            _ => {}
        };
        Ok(())
    }

    fn decode_configuration_set(&mut self, capture: &mut Capture, fields: &SetupFields)
        -> Result<(), CaptureError>
    {
        let dev_data = self.current_device_data_mut(capture)?;
        dev_data.config_number = Some(ConfigNum(fields.value.try_into()?));
        dev_data.update_endpoint_details();
        Ok(())
    }

    fn transfer_status(&mut self, capture: &mut Capture, next: PID)
        -> Result<DecodeStatus, CaptureError>
    {
        let endpoint_id = self.current_endpoint_id()?;
        let ep_data = self.current_endpoint_data()?;
        let dev_data = self.current_device_data(capture)?;
        let (ep_type, ep_max) = dev_data.endpoint_details(ep_data.address);
        let success = self.transaction_state.successful();
        let length = self.transaction_state.payload.len() as u64;
        let short = match ep_max {
            Some(max) => length < max as u64,
            None      => false
        };
        use PID::*;
        use EndpointType::{Normal, Framing};
        use usb::EndpointType::*;
        use Direction::*;
        use DecodeStatus::*;
        Ok(match (ep_type, ep_data.last, next) {

            // A SETUP transaction starts a new control transfer.
            // Store the setup fields to interpret the request.
            (Normal(Control), _, SETUP) => {
                use StartComplete::*;
                match self.transaction_state.split_sc {
                    None | Some(Start) => {
                        let setup = self.transaction_state.setup;
                        let ep_data = self.current_endpoint_data_mut()?;
                        ep_data.setup = setup;
                    },
                    Some(Complete) => {},
                };
                New
            },

            (Normal(Control), _, _) => match &ep_data.setup {
                // No control transaction is valid unless setup was done.
                None => Invalid,
                // If setup was done then valid transactions depend on the
                // contents of the setup data packet.
                Some(fields) => {
                    let with_data = fields.length != 0;
                    let direction = fields.type_fields.direction();
                    match (direction, with_data, ep_data.last, next) {

                        // If there is data to transfer, setup stage is
                        // followed by IN/OUT at data stage in the direction
                        // of the request. IN/OUT may then be repeated.
                        (In,  true, Some(SETUP), IN ) |
                        (Out, true, Some(SETUP), OUT) |
                        (In,  true, Some(IN),    IN ) |
                        (Out, true, Some(OUT),   OUT) => {
                            if success {
                                let payload =
                                    self.transaction_state.payload.clone();
                                let ep_data = self.current_endpoint_data_mut()?;
                                ep_data.payload.extend(payload);
                                // Await status stage.
                                Continue
                            } else {
                                // Retry data stage.
                                Retry
                            }
                        },

                        // If there is no data to transfer, setup stage is
                        // followed by IN/OUT at status stage in the opposite
                        // direction to the request. If there is data, then
                        // the status stage follows the data stage.
                        (In,  false, Some(SETUP), OUT) |
                        (Out, false, Some(SETUP), IN ) |
                        (In,  true,  Some(IN),    OUT) |
                        (Out, true,  Some(OUT),   IN ) => {
                            if success {
                                let fields_copy = *fields;
                                self.decode_request(capture, fields_copy)?;
                                // Status stage complete.
                                Done
                            } else {
                                // Retry status stage.
                                Retry
                            }
                        },

                        // PING is valid at any time that OUT would be.
                        (Out, true,  Some(SETUP), PING) |
                        (Out, true,  Some(OUT),   PING) |
                        (In,  false, Some(SETUP), PING) |
                        (In,  true,  Some(IN),    PING) => Retry,

                        // Any other sequence is invalid.
                        (..) => Invalid
                    }
                }
            },

            // An IN or OUT transaction on a non-control endpoint,
            // with no transfer in progress, starts a new transfer.
            // This can be either an actual transfer, or a polling
            // group used to collect NAKed transactions.
            (_, None, IN | OUT) => {
                let ep_traf = capture.endpoint_traffic(endpoint_id)?;
                ep_traf.data_index.push(ep_traf.total_data)?;
                if success {
                    ep_traf.total_data += length;
                }
                let ep_data = self.current_endpoint_data_mut()?;
                ep_data.last_success = success;
                if success && short {
                    // New transfer, ended immediately by a short packet.
                    Single
                } else {
                    // Either a new transfer or a new polling group.
                    New
                }
            },

            // IN or OUT may then be repeated.
            (_, Some(IN),  IN ) |
            (_, Some(OUT), OUT) => {
                let success_changed = success != ep_data.last_success;
                let ep_traf = capture.endpoint_traffic(endpoint_id)?;
                ep_traf.data_index.push(ep_traf.total_data)?;
                if success {
                    ep_traf.total_data += length;
                }
                if success_changed {
                    let ep_data = self.current_endpoint_data_mut()?;
                    ep_data.last_success = success;
                    if success && short {
                        // New transfer, ended immediately by a short packet.
                        Single
                    } else {
                        // Either a new transfer or a new polling group.
                        New
                    }
                } else if success {
                    // Continuing an ongoing transfer.
                    if short {
                        // A short packet ends the transfer.
                        Done
                    } else {
                        // A full-length packet continues the transfer.
                        Continue
                    }
                } else {
                    // Continuing a polling group.
                    Retry
                }
            },

            // OUT may be followed by PING.
            (_, Some(OUT), PING) => Retry,

            // A SOF group starts a special transfer, unless
            // one is already in progress.
            (Framing, None, SOF) => New,

            // Further SOF groups continue this transfer.
            (Framing, Some(SOF), SOF) => Continue,

            // Any other case is not a valid part of a transfer.
            _ => Invalid
        })
    }

    fn transfer_update(&mut self, capture: &mut Capture, transaction_id: TransactionId)
        -> Result<(), CaptureError>
    {
        use DecodeStatus::*;
        use StartComplete::*;
        let mut next = self.transaction_state.first.ok_or_else(||
            IndexError(String::from(
                "Transaction state has no first PID set")))?;
        if next == PID::SPLIT {
            next = self.transaction_state.split_pid.ok_or_else(||
                IndexError(String::from(
                    "Transaction state has no split PID set")))?;
        }
        match (self.transfer_status(capture, next)?,
               self.transaction_state.split_sc,
               self.transaction_state.successful())
        {
            (Single, None, _) => {
                self.transfer_start(capture, transaction_id, next, true)?;
                self.transfer_end(capture)?;
            },
            (Single, Some(Start), true) => {
                self.transfer_start(capture, transaction_id, next, false)?;
            },
            (Single, Some(Start), false) => {
                self.transfer_start(capture, transaction_id, next, false)?;
                self.transfer_end(capture)?;
            },
            (Single, Some(Complete), true) => {
                self.transfer_append(capture, transaction_id, next, true)?;
            },
            (Single, Some(Complete), false) => {
                self.transfer_append(capture, transaction_id, next, false)?;
                self.transfer_end(capture)?;
            },
            (New, None, _) => {
                self.transfer_start(capture, transaction_id, next, true)?;
            },
            (New, Some(Start), true) => {
                self.transfer_start(capture, transaction_id, next, false)?;
            },
            (New, Some(Start), false) => {
                self.transfer_start(capture, transaction_id, next, false)?;
                self.transfer_end(capture)?;
            },
            (New, Some(Complete), true) => {
                self.transfer_append(capture, transaction_id, next, true)?;
            },
            (New, Some(Complete), false) => {
                self.transfer_append(capture, transaction_id, next, true)?;
                self.transfer_end(capture)?;
            },
            (Continue, None, _) |
            (Continue, Some(Complete), true) => {
                self.transfer_append(capture, transaction_id, next, true)?;
            },
            (Continue, ..) => {
                self.transfer_append(capture, transaction_id, next, false)?;
            },
            (Retry, ..) => {
                self.transfer_append(capture, transaction_id, next, false)?;
            },
            (Done, None, _) |
            (Done, Some(Complete), true) => {
                self.transfer_append(capture, transaction_id, next, true)?;
                self.transfer_end(capture)?;
            },
            (Done, ..) => {
                self.transfer_append(capture, transaction_id, next, false)?;
            }
            (Invalid, ..) => {
                self.transfer_start(capture, transaction_id, next, false)?;
                self.transfer_end(capture)?;
            }
        }
        Ok(())
    }

    fn transfer_start(&mut self,
                      capture: &mut Capture,
                      transaction_id: TransactionId,
                      transaction_type: PID,
                      success: bool)
        -> Result<(), CaptureError>
    {
        let endpoint_id = self.current_endpoint_id()?;
        let ep_data = self.current_endpoint_data()?;
        if ep_data.transaction_count > 0 {
            self.add_transfer_entry(capture, endpoint_id, false)?;
            let ep_data = self.current_endpoint_data_mut()?;
            ep_data.ended = ep_data.active.take();
        }
        let ep_traf = capture.endpoint_traffic(endpoint_id)?;
        let ep_transaction_id = ep_traf.transaction_ids.push(transaction_id)?;
        let ep_transfer_id = ep_traf.transfer_index.push(ep_transaction_id)?;
        let ep_data = self.current_endpoint_data_mut()?;
        ep_data.active = Some(ep_transfer_id);
        ep_data.transaction_count = 1;
        if success {
            ep_data.last = Some(transaction_type);
        } else {
            ep_data.last = None
        }
        ep_data.payload.clear();
        let transfer_start_id = self.add_transfer_entry(capture, endpoint_id, true)?;
        self.add_item(capture, endpoint_id, transfer_start_id, true)?;
        Ok(())
    }

    fn transfer_append(&mut self,
                       capture: &mut Capture,
                       transaction_id: TransactionId,
                       transaction_type: PID,
                       success: bool)
        -> Result<(), CaptureError>
    {
        let endpoint_id = self.current_endpoint_id()?;
        let ep_traf = capture.endpoint_traffic(endpoint_id)?;
        ep_traf.transaction_ids.push(transaction_id)?;
        let ep_data = self.current_endpoint_data_mut()?;
        ep_data.transaction_count += 1;
        if success {
            ep_data.last = Some(transaction_type);
        }
        Ok(())
    }

    fn transfer_end(&mut self, capture: &mut Capture)
        -> Result<(), CaptureError>
    {
        let endpoint_id = self.current_endpoint_id()?;
        let ep_data = self.current_endpoint_data()?;
        if ep_data.transaction_count > 0 {
            let transfer_end_id =
                self.add_transfer_entry(capture, endpoint_id, false)?;
            if self.last_item_endpoint != Some(endpoint_id) {
                self.add_item(capture, endpoint_id, transfer_end_id, false)?;
            }
            let ep_data = self.current_endpoint_data_mut()?;
            ep_data.ended = ep_data.active.take();
        }
        let ep_data = self.current_endpoint_data_mut()?;
        ep_data.transaction_count = 0;
        ep_data.last = None;
        ep_data.payload.clear();
        Ok(())
    }

    fn add_transfer_entry(&mut self,
                          capture: &mut Capture,
                          endpoint_id: EndpointId,
                          start: bool)
        -> Result<TransferId, CaptureError>
    {
        let ep_data = self.current_endpoint_data()?;
        let ep_transfer_id = ep_data.active.ok_or_else(||
            IndexError(format!(
                "No active transfer on endpoint {}", endpoint_id)))?;
        self.add_endpoint_state(capture, endpoint_id, start)?;
        let mut entry = TransferIndexEntry::default();
        entry.set_endpoint_id(endpoint_id);
        entry.set_transfer_id(ep_transfer_id);
        entry.set_is_start(start);
        let transfer_id = capture.transfer_index.push(&entry)?;
        Ok(transfer_id)
    }

    fn add_endpoint_state(&mut self,
                          capture: &mut Capture,
                          endpoint_id: EndpointId,
                          start: bool)
        -> Result<TransferId, CaptureError>
    {
        let endpoint_count = capture.endpoints.len() as usize;
        for i in 0..endpoint_count {
            use EndpointState::*;
            self.last_endpoint_state[i] = {
                let same = i == endpoint_id.value as usize;
                let last = EndpointState::from(self.last_endpoint_state[i]);
                match (same, start, last) {
                    (true, true,  _)               => Starting,
                    (true, false, _)               => Ending,
                    (false, _, Starting | Ongoing) => Ongoing,
                    (false, _, Ending | Idle)      => Idle,
                }
            } as u8;
        }
        let last_state = self.last_endpoint_state.as_slice();
        let state_offset = capture.endpoint_states.append(last_state)?;
        let state_id = capture.endpoint_state_index.push(state_offset)?;
        Ok(state_id)
    }

    fn add_item(&mut self,
                 capture: &mut Capture,
                 item_endpoint_id: EndpointId,
                 transfer_id: TransferId,
                 start: bool)
        -> Result<TrafficItemId, CaptureError>
    {
        let item_id = capture.item_index.push(transfer_id)?;
        self.last_item_endpoint = Some(item_endpoint_id);

        // Look for ended transfers which still need to be linked to an item.
        let endpoint_count = capture.endpoints.len();
        for i in 0..endpoint_count {
            let endpoint_id = EndpointId::from_u64(i);
            let ep_data = self.endpoint_data
                .get_mut(endpoint_id)
                .ok_or_else(|| IndexError(
                    format!("Endpoint {} has no associated data", i)))?;
            if start && endpoint_id == item_endpoint_id &&
                ep_data.start_item.replace(item_id).is_none()
            {
                let ep_traf = capture.endpoint_traffic(endpoint_id)?;
                ep_traf.first_item_id = Some(item_id);
            }
            if let Some(ep_transfer_id) = ep_data.ended.take() {
                // This transfer has ended and is not yet linked to an item.
                let ep_traf = capture.endpoint_traffic(endpoint_id)?;
                assert!(ep_traf.end_index.push(item_id)? == ep_transfer_id);
            }
            let ep_traf = capture.endpoint_traffic(endpoint_id)?;
            if ep_traf.first_item_id.is_some() {
                // Record the total transactions on this endpoint.
                let mut transaction_count = ep_traf.transaction_ids.len();
                if start && endpoint_id == item_endpoint_id {
                    // We just added a transaction, that shouldn't be included.
                    transaction_count -= 1;
                }
                ep_traf.progress_index.push(
                    EndpointTransactionId::from_u64(transaction_count))?;
            }
        }

        Ok(item_id)
    }
}
