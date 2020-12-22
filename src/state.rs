use std::str::FromStr;

use anyhow::Result;
use either::Either;
use serde::{Deserialize, Serialize};
use sled::Tree;

use crate::settings;

pub struct State {
    address_by_chat: Tree,
    address_comments: Tree,
    masterchain_incoming: Tree,
    masterchain_outgoing: Tree,
    basechain_incoming: Tree,
    basechain_outgoing: Tree,
}

const ADDRESS_BY_CHAT_PREFIX: u8 = 250u8;
const ADDRESS_COMMENTS_PREFIX: u8 = 249u8;
const MASTERCHAIN: i8 = -1;
const BASECHAIN: i8 = 0;

impl State {
    pub fn new(settings: settings::Db) -> Result<Self> {
        let db = sled::open(settings.path)?;
        let address_by_chat = db.open_tree(&[ADDRESS_BY_CHAT_PREFIX])?;
        let address_comments = db.open_tree(&[ADDRESS_COMMENTS_PREFIX])?;
        let masterchain_incoming = db.open_tree(&[MASTERCHAIN as u8, 0])?;
        let masterchain_outgoing = db.open_tree(&[MASTERCHAIN as u8, 1])?;
        let basechain_incoming = db.open_tree(&[BASECHAIN as u8, 0])?;
        let basechain_outgoing = db.open_tree(&[BASECHAIN as u8, 1])?;

        Ok(Self {
            address_by_chat,
            address_comments,
            masterchain_incoming,
            masterchain_outgoing,
            basechain_incoming,
            basechain_outgoing,
        })
    }

    pub fn get_comment(&self, chat_id: i64, workchain: i8, addr: &[u8]) -> Result<Option<String>> {
        let mut key = [0; 41]; // 8 bytes chat id, 1 byte workchain, 32 bytes address
        key[0..8].copy_from_slice(&chat_id.to_be_bytes());
        key[8] = workchain as u8;
        key[9..41].copy_from_slice(addr);

        match self.address_comments.get(key)? {
            Some(comment) => {
                let comment = String::from_utf8(comment.to_vec())?;
                Ok(Some(comment))
            }
            None => Ok(None),
        }
    }

    pub fn set_comment(&self, chat_id: i64, addr: &str, comment: &str) -> Result<()> {
        let mut key = [0; 41]; // 8 bytes chat id, 1 byte workchain, 32 bytes address
        key[0..8].copy_from_slice(&chat_id.to_be_bytes());

        let mut workchain = 0;
        parse_address(addr, &mut workchain, &mut key[9..41])?;
        key[8] = workchain as u8;

        self.address_comments.insert(key, comment.as_bytes())?;

        Ok(())
    }

    pub fn remove_comment(&self, chat_id: i64, addr: &str) -> Result<()> {
        let mut key = [0; 41]; // 8 bytes chat id, 1 byte workchain, 32 bytes address
        key[0..8].copy_from_slice(&chat_id.to_be_bytes());

        let mut workchain = 0;
        parse_address(addr, &mut workchain, &mut key[9..41])?;
        key[8] = workchain as u8;

        self.address_comments.remove(key)?;

        Ok(())
    }

    pub fn subscriptions(
        &self,
        chat_id: i64,
    ) -> impl Iterator<Item = (i8, Vec<u8>, Direction, SubscriptionFilter)> {
        let masterchain_subscriptions =
            iter_subscriptions(&self.address_by_chat, chat_id, MASTERCHAIN);
        let basechain_subscriptions = iter_subscriptions(&self.address_by_chat, chat_id, BASECHAIN);

        masterchain_subscriptions.chain(basechain_subscriptions)
    }

    pub fn subscribers_incoming(
        &self,
        workchain: i8,
        addr: &[u8],
    ) -> impl Iterator<Item = (i64, SubscriptionFilter)> {
        match workchain {
            MASTERCHAIN => Either::Left(iter_chats(&self.masterchain_incoming, addr)),
            BASECHAIN => Either::Right(iter_chats(&self.basechain_incoming, addr)),
            _ => unreachable!(),
        }
    }

    pub fn subscribers_outgoing(
        &self,
        workchain: i8,
        addr: &[u8],
    ) -> impl Iterator<Item = (i64, SubscriptionFilter)> {
        match workchain {
            MASTERCHAIN => Either::Left(iter_chats(&self.masterchain_outgoing, addr)),
            BASECHAIN => Either::Right(iter_chats(&self.basechain_outgoing, addr)),
            _ => unreachable!(),
        }
    }

    pub fn insert(
        &self,
        addr: &str,
        direction: Direction,
        filter: SubscriptionFilter,
        chat_id: i64,
    ) -> Result<()> {
        let mut key = [0; 41]; // 32 bytes address, 8 bytes chat id, 1 byte workchain
        let mut workchain = 0;
        parse_address(addr, &mut workchain, &mut key[0..32])?;

        key[32..40].copy_from_slice(&chat_id.to_le_bytes());

        let with_incoming = direction == Direction::All || direction == Direction::Incoming;
        let with_outgoing = direction == Direction::All || direction == Direction::Outgoing;

        let filter = bincode::serialize(&filter).unwrap();

        match workchain {
            MASTERCHAIN => {
                if with_incoming {
                    self.masterchain_incoming
                        .insert(&key[0..40], filter.clone())?;
                }
                if with_outgoing {
                    self.masterchain_outgoing
                        .insert(&key[0..40], filter.clone())?;
                }
            }
            BASECHAIN => {
                if with_incoming {
                    self.basechain_incoming
                        .insert(&key[0..40], filter.clone())?;
                }
                if with_outgoing {
                    self.basechain_outgoing
                        .insert(&key[0..40], filter.clone())?;
                }
            }
            _ => {}
        };

        key[40] = workchain as u8;
        key.rotate_right(9); // shift elements, so key will be [chat id (8 bytes), workchain (1 byte), addr]

        let mut value = Vec::with_capacity(filter.len() + 1);
        value.push(direction.as_byte());
        value.extend(filter.into_iter());

        self.address_by_chat
            .update_and_fetch(&key, |old| match old {
                Some(old_value) if !old_value.is_empty() => {
                    value[0] |= old_value[0];
                    Some(value.clone())
                }
                _ => Some(value.clone()),
            })?;

        Ok(())
    }

    pub fn remove(&self, addr: &str, direction: Direction, chat_id: i64) -> Result<()> {
        let mut key = [0; 41]; // 32 bytes address, 8 bytes chat id, 1 byte workchain
        let mut workchain = 0;
        parse_address(addr, &mut workchain, &mut key[0..32])?;

        key[32..40].copy_from_slice(&chat_id.to_le_bytes());

        let with_incoming = direction == Direction::All || direction == Direction::Incoming;
        let with_outgoing = direction == Direction::All || direction == Direction::Outgoing;

        match workchain {
            MASTERCHAIN => {
                if with_incoming {
                    self.masterchain_incoming.remove(&key[0..40])?;
                }
                if with_outgoing {
                    self.masterchain_outgoing.remove(&key[0..40])?;
                }
            }
            BASECHAIN => {
                if with_incoming {
                    self.basechain_incoming.remove(&key[0..40])?;
                }
                if with_outgoing {
                    self.basechain_outgoing.remove(&key[0..40])?;
                }
            }
            _ => {}
        };

        key[40] = workchain as u8;
        key.rotate_right(9); // shift elements, so key will be [chat id (8 bytes), workchain (1 byte), addr]

        let value = self
            .address_by_chat
            .update_and_fetch(&key, |old| match old {
                Some(old_value) if !old_value.is_empty() => {
                    let mut new_value = Vec::from(old_value);
                    new_value[0] &= !direction.as_byte();
                    Some(new_value)
                }
                _ => None,
            })?;

        if matches!(value, Some(current) if !current.is_empty() && current[0] == 0) {
            self.address_by_chat.remove(&key)?;
        }

        Ok(())
    }
}

fn iter_subscriptions(
    db: &Tree,
    chat_id: i64,
    workchain: i8,
) -> impl Iterator<Item = (i8, Vec<u8>, Direction, SubscriptionFilter)> {
    let mut prefix = [0; 9];
    prefix[0..8].copy_from_slice(&chat_id.to_le_bytes());
    prefix[8] = workchain as u8;

    db.scan_prefix(&prefix).filter_map(|item| {
        item.ok().and_then(|(key, value)| {
            if key.len() != 41 || value.is_empty() {
                return None;
            }
            let direction = Direction::from_byte(value[0]);

            let filter = bincode::deserialize(&value[1..]).unwrap_or_default();

            let workchain = key[8] as i8;
            let addr = key[9..].to_vec();

            Some((workchain, addr, direction, filter))
        })
    })
}

fn iter_chats(db: &Tree, prefix: &[u8]) -> impl Iterator<Item = (i64, SubscriptionFilter)> {
    db.scan_prefix(prefix).filter_map(|item| {
        item.ok().map(|(key, value)| {
            let mut bytes = [0; 8];
            bytes.copy_from_slice(&key[32..40]);

            let chat_id = i64::from_le_bytes(bytes);
            let filter = bincode::deserialize(&value).unwrap_or_default();

            (chat_id, filter)
        })
    })
}

#[derive(Debug, Copy, Clone, Default, Deserialize, Serialize)]
pub struct SubscriptionFilter {
    pub gt: Option<u64>,
    pub lt: Option<u64>,
}

impl std::fmt::Display for SubscriptionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.gt, self.lt) {
            (Some(gt), None) => f.write_fmt(format_args!("\\>\\= {}", Tons(gt))),
            (None, Some(lt)) => f.write_fmt(format_args!("\\<\\= {}", Tons(lt))),
            (Some(gt), Some(lt)) => {
                f.write_fmt(format_args!("\\>\\= {} and \\<\\= {}", Tons(gt), Tons(lt)))
            }
            _ => Ok(()),
        }
    }
}

pub struct Tons(pub u64);

impl std::fmt::Display for Tons {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let int = self.0 / 1000000000;
        let frac = self.0 % 1000000000;

        if frac == 0 {
            f.write_fmt(format_args!("{}", int))
        } else {
            f.write_fmt(format_args!(
                "{}\\.{}",
                int,
                format!("{:09}", frac).trim_end_matches('0')
            ))
        }
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum Direction {
    All,
    Incoming,
    Outgoing,
}

impl Direction {
    fn as_byte(self) -> u8 {
        match self {
            Direction::All => 3,
            Direction::Incoming => 1,
            Direction::Outgoing => 2,
        }
    }

    fn from_byte(byte: u8) -> Self {
        match byte {
            1 => Direction::Incoming,
            2 => Direction::Outgoing,
            _ => Direction::All,
        }
    }
}

impl FromStr for Direction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(Direction::All),
            "incoming" => Ok(Direction::Incoming),
            "outgoing" => Ok(Direction::Outgoing),
            _ => Err(anyhow!("invalid direction name")),
        }
    }
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::All => f.write_str("all"),
            Direction::Incoming => f.write_str("incoming"),
            Direction::Outgoing => f.write_str("outgoing"),
        }
    }
}

fn parse_address(addr: &str, workchain: &mut i8, key: &mut [u8]) -> Result<()> {
    match addr.len() {
        66 | 67 => parse_raw_address(addr, workchain, key),
        48 => parse_packed_address(addr, workchain, key),
        _ => Err(anyhow!("invalid address")),
    }
}

fn parse_raw_address(addr: &str, workchain: &mut i8, key: &mut [u8]) -> Result<()> {
    let mut parts = addr.split(':');

    *workchain = parts
        .next()
        .ok_or_else(|| anyhow!("failed to parse workchain"))
        .and_then(|workchain| i8::from_str(workchain).map_err(anyhow::Error::from))
        .and_then(validate_workchain)?;

    let address = parts
        .next()
        .ok_or_else(|| anyhow!("failed to parse hash"))?;

    hex::decode_to_slice(address, key)?;

    Ok(())
}

fn parse_packed_address(addr: &str, workchain: &mut i8, key: &mut [u8]) -> Result<()> {
    let bytes = base64::decode(addr)?;
    if bytes.len() != 36 {
        return Err(anyhow!("invalid packed address length"));
    }

    *workchain = validate_workchain(bytes[1] as i8)?;
    key.copy_from_slice(&bytes[2..34]);

    Ok(())
}

fn validate_workchain(workchain: i8) -> Result<i8> {
    match workchain {
        MASTERCHAIN | BASECHAIN => Ok(workchain),
        _ => Err(anyhow!("invalid address workchain")),
    }
}
