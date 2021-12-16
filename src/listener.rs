use std::sync::Arc;

use anyhow::Result;
use chrono::NaiveDateTime;
use either::Either;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::{ClientConfig, Message};
use serde::Deserialize;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, ReplyMarkup};
use teloxide::Bot;
use tokio_stream::StreamExt;
use ton_block::Deserializable;
use ton_block_compressor::ZstdWrapper;

use crate::settings;
use crate::state::*;

pub fn spawn_listener(settings: settings::Kafka, bot: Bot, state: Arc<State>) -> Result<()> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", &settings.consumer_group_id)
        .set("bootstrap.servers", &settings.bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("security.protocol", &settings.security_protocol)
        .set("ssl.ca.location", &settings.ssl_ca_location)
        .set("sasl.mechanism", &settings.sasl_mechanism)
        .set("sasl.username", &settings.sasl_username)
        .set("sasl.password", &settings.sasl_password);

    let consumers = (0..settings.partition_count).map(|partition| {
        let mut assignment = rdkafka::TopicPartitionList::new();
        let consumer: StreamConsumer = assignment
            .add_partition_offset(
                &settings.transactions_topic,
                partition as i32,
                rdkafka::Offset::End,
            )
            .and_then(|_| client_config.create())
            .unwrap_or_else(|e| {
                panic!(
                    "Consumer creation failed for partition {} - {}",
                    partition, e
                )
            });

        consumer.assign(&assignment).unwrap();
        consumer
    });

    for consumer in consumers.into_iter() {
        tokio::spawn(listen_consumer(consumer, bot.clone(), state.clone()));
    }

    Ok(())
}

async fn listen_consumer(consumer: StreamConsumer, bot: Bot, state: Arc<State>) {
    let mut zstd = ZstdWrapper::new();
    loop {
        let mut messages = consumer.stream();
        log::debug!("Started consumer {:?}", consumer.assignment());

        while let Some(message) = messages.next().await {
            match message {
                Ok(message) => {
                    let payload = match message.payload().map(|data| zstd.decompress(data)) {
                        Some(Ok(data)) => data,
                        _ => continue,
                    };

                    let transaction = match parse_transaction(payload) {
                        Ok(Some(tx)) => tx,
                        Ok(None) => continue,
                        Err(e) => {
                            log::error!("failed to parse transaction: {:?}", e);
                            continue;
                        }
                    };

                    match &transaction.message_in {
                        Some(msg) => {
                            if let Err(e) = process_message(
                                &transaction,
                                msg,
                                &bot,
                                state.as_ref(),
                                TransferDirection::Incoming,
                            )
                            .await
                            {
                                log::debug!("error processing incoming message: {:?}", e);
                            }
                        }
                        _ => continue,
                    };

                    for msg in &transaction.messages_out {
                        if let Err(e) = process_message(
                            &transaction,
                            msg,
                            &bot,
                            state.as_ref(),
                            TransferDirection::Outgoing,
                        )
                        .await
                        {
                            log::debug!("error processing outgoing message: {:?}", e);
                        }
                    }

                    if let Err(e) =
                        consumer.commit_message(&message, rdkafka::consumer::CommitMode::Async)
                    {
                        log::error!("Failed to commit message: {:?}", e);
                    }
                }
                Err(err) => {
                    log::error!("Kafka error: {:?}", err);
                }
            }
        }

        log::debug!("Done {:?}", consumer.assignment());
    }
}

fn parse_transaction(payload: &[u8]) -> Result<Option<Transaction>> {
    fn convert_address(address: &ton_block::MsgAddressInt) -> MessageAddress {
        MessageAddress {
            workchain: address.workchain_id() as i8,
            address: hex::encode(address.address().get_bytestring(0)),
        }
    }

    fn convert_message(msg: ton_block::Message) -> Result<Option<TransactionMessage>> {
        Ok(Some(TransactionMessage {
            info: match msg.header() {
                ton_block::CommonMsgInfo::ExtInMsgInfo(_) => TransactionMessageInfo::ExternalIn,
                ton_block::CommonMsgInfo::IntMsgInfo(info) => TransactionMessageInfo::Internal {
                    src: match &info.src {
                        ton_block::MsgAddressIntOrNone::Some(addr) => convert_address(addr),
                        ton_block::MsgAddressIntOrNone::None => return Ok(None),
                    },
                    dest: convert_address(&info.dst),
                    bounce: info.bounce,
                    value: info.value.grams.0 as u64,
                },
                ton_block::CommonMsgInfo::ExtOutMsgInfo(_) => TransactionMessageInfo::ExternalOut,
            },
        }))
    }

    let cell = ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(payload))?;
    let hash = cell.repr_hash();
    let tx = ton_block::Transaction::construct_from(&mut cell.into())?;

    let in_msg = match tx.in_msg {
        Some(msg) => msg.read_struct()?,
        None => return Ok(None),
    };

    let description = match tx.description.read_struct()? {
        ton_block::TransactionDescr::Ordinary(descr) => TransactionDescription {
            aborted: descr.aborted,
        },
        _ => return Ok(None),
    };

    let mut messages_out = Vec::with_capacity(tx.outmsg_cnt as usize);
    tx.out_msgs.iterate(|item| {
        if let Some(msg) = convert_message(item.0)? {
            messages_out.push(msg);
        }
        Ok(true)
    })?;

    Ok(Some(Transaction {
        now: tx.now as i64,
        hash: hash.to_hex_string(),
        description,
        message_in: convert_message(in_msg)?,
        messages_out,
    }))
}

async fn process_message(
    transaction: &Transaction,
    message: &TransactionMessage,
    bot: &Bot,
    state: &State,
    direction: TransferDirection,
) -> Result<()> {
    if let TransactionMessageInfo::Internal {
        src,
        dest,
        bounce,
        value,
        ..
    } = &message.info
    {
        let time = NaiveDateTime::from_timestamp(transaction.now, 0)
            .format("%Y\\-%m\\-%d %H:%M:%S UTC")
            .to_string();

        let bounced = transaction.description.aborted && *bounce;
        let text = TransferResponse {
            time,
            direction,
            bounced,
            src,
            dest,
            value,
        };

        let markup = transaction.make_reply_markup();

        let src_addr = hex::decode(&src.address).unwrap();
        let dest_addr = hex::decode(&dest.address).unwrap();

        let chat_ids = match direction {
            TransferDirection::Incoming => {
                Either::Left(state.subscribers_incoming(dest.workchain, &dest_addr))
            }
            TransferDirection::Outgoing => {
                Either::Right(state.subscribers_outgoing(src.workchain, &src_addr))
            }
        };

        for (chat_id, filter) in chat_ids {
            if matches!(filter.gt, Some(ref gt) if value < gt)
                || matches!(filter.lt, Some(ref lt) if value > lt)
            {
                continue;
            }

            let src_comment = state
                .get_comment(chat_id, src.workchain, &src_addr)
                .ok()
                .flatten();
            let dest_comment = state
                .get_comment(chat_id, dest.workchain, &dest_addr)
                .ok()
                .flatten();

            send_message(
                bot,
                chat_id,
                text.with_comments(src_comment, dest_comment),
                &markup,
            )
            .await;
        }
    }

    Ok(())
}

async fn send_message<T>(bot: &Bot, chat_id: i64, text: T, markup: &ReplyMarkup)
where
    T: Into<String>,
{
    if let Err(e) = bot
        .send_message(chat_id, text)
        .reply_markup(markup.clone())
        .parse_mode(ParseMode::MarkdownV2)
        .send()
        .await
    {
        log::error!("failed to send message: {:?}", e);
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Transaction {
    now: i64,
    hash: String,
    description: TransactionDescription,
    message_in: Option<TransactionMessage>,
    messages_out: Vec<TransactionMessage>,
}

impl Transaction {
    fn make_reply_markup(&self) -> ReplyMarkup {
        ReplyMarkup::InlineKeyboard(InlineKeyboardMarkup::default().append_row(vec![
            InlineKeyboardButton::url(
                "View in explorer".to_owned(),
                format!("https://tonscan.io/transactions/{}", self.hash),
            ),
        ]))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TransactionMessage {
    info: TransactionMessageInfo,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum TransactionMessageInfo {
    ExternalIn,
    Internal {
        src: MessageAddress,
        dest: MessageAddress,
        bounce: bool,
        value: u64,
    },
    ExternalOut,
}

#[derive(Debug, Clone, Deserialize)]
struct TransactionDescription {
    aborted: bool,
}

struct TransferResponseWithComments<'a, 'r> {
    info: &'a TransferResponse<'r>,
    src_comment: Option<String>,
    dest_comment: Option<String>,
}

impl<'a, 'r> std::fmt::Display for TransferResponseWithComments<'a, 'r> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.info.direction {
            TransferDirection::Incoming if self.info.bounced => {
                f.write_str("❌ Incoming transfer (bounced!)\\. ")?
            }
            TransferDirection::Incoming => f.write_str("📨 Incoming transfer\\. ")?,
            TransferDirection::Outgoing => f.write_str("💸 Outgoing transfer\\. ")?,
        };

        f.write_str(&self.info.time)?;

        match &self.src_comment {
            Some(comment) => f.write_fmt(format_args!(
                "\n\nFrom \\({}\\):\n{}",
                comment, self.info.src
            ))?,
            None => f.write_fmt(format_args!("\n\nFrom:\n{}", self.info.src))?,
        }

        match &self.dest_comment {
            Some(comment) => f.write_fmt(format_args!(
                "\n\nTo \\({}\\):\n{}",
                comment, self.info.dest
            ))?,
            None => f.write_fmt(format_args!("\n\nTo:\n{}", self.info.dest))?,
        }

        f.write_str("\n\n💎 ")?;
        Tons(*self.info.value).fmt(f)
    }
}

impl<'a, 'r> From<TransferResponseWithComments<'a, 'r>> for String {
    fn from(r: TransferResponseWithComments<'a, 'r>) -> Self {
        r.to_string()
    }
}

struct TransferResponse<'a> {
    time: String,
    direction: TransferDirection,
    bounced: bool,
    src: &'a MessageAddress,
    dest: &'a MessageAddress,
    value: &'a u64,
}

impl<'a> TransferResponse<'a> {
    fn with_comments(
        &self,
        src_comment: Option<String>,
        dest_comment: Option<String>,
    ) -> TransferResponseWithComments {
        TransferResponseWithComments {
            info: self,
            src_comment,
            dest_comment,
        }
    }
}

#[derive(Copy, Clone)]
enum TransferDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Deserialize)]
struct MessageAddress {
    workchain: i8,
    address: String,
}

impl std::fmt::Display for MessageAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "`{}:{}`",
            self.workchain,
            self.address.to_lowercase()
        ))
    }
}
