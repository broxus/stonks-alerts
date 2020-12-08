use std::sync::Arc;

use anyhow::Result;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::{ClientConfig, Message};
use serde::export::Formatter;
use serde::Deserialize;
use teloxide::prelude::Request;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, ReplyMarkup};
use teloxide::Bot;
use tokio::stream::StreamExt;

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
        assignment.add_partition_offset(
            &settings.transactions_topic,
            partition as i32,
            rdkafka::Offset::End,
        );

        let consumer: StreamConsumer = client_config.create().unwrap_or_else(|e| {
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
    loop {
        let mut messages = consumer.start();
        log::debug!("Started consumer {:?}", consumer.assignment());

        while let Some(message) = messages.next().await {
            match message {
                Ok(message) => {
                    let payload = match message.payload() {
                        Some(data) => data,
                        None => continue,
                    };

                    let transaction = match serde_json::from_slice::<Transaction>(payload) {
                        Ok(transaction) => transaction,
                        Err(e) => {
                            log::error!("failed to parse transaction: {:?}", e);
                            continue;
                        }
                    };

                    match &transaction.message_in {
                        Some(msg) => {
                            if let Err(e) = process_message(
                                &transaction,
                                &msg,
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
                            &msg,
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
        let bounced = transaction.description.aborted && *bounce;
        let text = TransferResponse {
            direction,
            bounced,
            src,
            dest,
            value,
        };

        let markup = transaction.make_reply_markup();

        match direction {
            TransferDirection::Incoming => {
                let addr = hex::decode(&dest.address).unwrap();
                for chat_id in state.subscribers_incoming(dest.workchain, &addr) {
                    send_message(bot, chat_id, text, &markup).await;
                }
            }
            TransferDirection::Outgoing => {
                let addr = hex::decode(&src.address).unwrap();
                for chat_id in state.subscribers_outgoing(src.workchain, &addr) {
                    send_message(bot, chat_id, text, &markup).await;
                }
            }
        }
    }

    Ok(())
}

async fn send_message(bot: &Bot, chat_id: i64, text: TransferResponse<'_>, markup: &ReplyMarkup) {
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
    workchain: i8,
    account: String,
    hash: String,
    description: TransactionDescription,
    message_in: Option<TransactionMessage>,
    messages_out: Vec<TransactionMessage>,
}

impl Transaction {
    fn make_reply_markup(&self) -> ReplyMarkup {
        ReplyMarkup::InlineKeyboardMarkup(InlineKeyboardMarkup::default().append_row(vec![
            InlineKeyboardButton::url(
                "View in explorer".to_owned(),
                format!("https://ton-explorer.com/transactions/{}", self.hash),
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
    ExternalIn {
        dest: MessageAddress,
    },
    Internal {
        src: MessageAddress,
        dest: MessageAddress,
        bounce: bool,
        bounced: bool,
        value: u64,
    },
    ExternalOut,
}

#[derive(Debug, Clone, Deserialize)]
struct TransactionDescription {
    aborted: bool,
    destroyed: bool,
    action: Option<TransactionActionPhase>,
}

#[derive(Debug, Clone, Deserialize)]
struct TransactionActionPhase {
    success: bool,
}

#[derive(Copy, Clone)]
struct TransferResponse<'a> {
    direction: TransferDirection,
    bounced: bool,
    src: &'a MessageAddress,
    dest: &'a MessageAddress,
    value: &'a u64,
}

impl<'a> std::fmt::Display for TransferResponse<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.direction {
            TransferDirection::Incoming if self.bounced => {
                f.write_str("❌ Incoming transfer (bounced!)")?
            }
            TransferDirection::Incoming => f.write_str("📨 Incoming transfer")?,
            TransferDirection::Outgoing => f.write_str("💸 Outgoing transfer")?,
        };

        f.write_fmt(format_args!("\n\nFrom:\n{}", self.src))?;
        f.write_fmt(format_args!("\n\nTo:\n{}", self.dest))?;
        Tons(*self.value).fmt(f)
    }
}

impl<'a> From<TransferResponse<'a>> for String {
    fn from(r: TransferResponse<'a>) -> Self {
        r.to_string()
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("`{}:{}`", self.workchain, self.address))
    }
}

struct Tons(u64);

impl std::fmt::Display for Tons {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let int = self.0 / 1000000000;
        let frac = self.0 % 1000000000;

        if frac == 0 {
            f.write_fmt(format_args!("\n\n💎 {}", int))
        } else {
            f.write_fmt(format_args!(
                "\n\n💎 {}\\.{}",
                int,
                format!("{:09}", frac).trim_end_matches('0')
            ))
        }
    }
}
