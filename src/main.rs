#[macro_use]
extern crate anyhow;

mod listener;
mod settings;
mod state;

use std::str::FromStr;
use std::sync::Arc;

use settings::Settings;
use teloxide::prelude::*;
use teloxide::types::ParseMode;
use teloxide::utils::command::{BotCommand, ParseError};

use crate::listener::*;
use crate::state::*;

struct Subscription {
    direction: Direction,
    address: String,
}

fn parse_subscription(input: String) -> Result<(Subscription,), ParseError> {
    let mut parts = input.trim().split(' ').filter(|s| !s.is_empty());

    let description = "Usage:\n    `/subscribe DIRECTION ADDRESS`\n\nWhere:\n    `DIRECTION` \\- `all`, `incoming`, `outgoing`\n    `ADDRESS` \\- raw or packed address";

    match parts.next() {
        Some(first) => match parts.next() {
            Some(second) => match Direction::from_str(first) {
                Ok(direction) => Ok((Subscription {
                    direction,
                    address: second.to_string(),
                },)),
                Err(e) => Err(ParseError::IncorrectFormat(
                    format!("{}\n\n{}", e.to_string(), description).into(),
                )),
            },
            None => Ok((Subscription {
                direction: Direction::All,
                address: first.to_owned(),
            },)),
        },
        None => Err(ParseError::Custom(description.to_owned().into())),
    }
}

#[derive(BotCommand)]
#[command(rename = "lowercase", description = "These commands are supported:")]
enum Command {
    #[command(description = "display this text")]
    Start,
    #[command(
        description = "subscribe to account",
        parse_with = "parse_subscription"
    )]
    Subscribe(Subscription),
    #[command(
        description = "unsubscribe from account",
        parse_with = "parse_subscription"
    )]
    Unsubscribe(Subscription),
    #[command(description = "List subscriptions")]
    List,
}

fn init_logger() {
    let log_filters = std::env::var("RUST_LOG").unwrap_or_default();

    pretty_env_logger::formatted_builder()
        .parse_filters(&log_filters)
        .format(|formatter, record| {
            use std::io::Write;
            writeln!(
                formatter,
                "{} [{}] - {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .init()
}

async fn run() -> anyhow::Result<()> {
    let settings = Settings::new()?;

    let bot = Bot::builder().token(settings.telegram.token).build();
    let me = bot.get_me().send().await?;
    let bot_name = me.user.username.ok_or_else(|| anyhow!("i'm not a bot"))?;

    let state = Arc::new(State::new(settings.db)?);

    spawn_listener(settings.kafka, bot.clone(), state.clone())?;

    teloxide::repl(bot, move |cx| {
        let state = state.clone();
        let bot_name = bot_name.clone();

        async move {
            let chat_id = cx.update.chat_id();
            let text = match cx.update.text() {
                Some(text) => text,
                None => return Ok(()),
            };

            match Command::parse(text, &bot_name) {
                Ok(Command::Start) => {
                    cx.answer(Command::descriptions()).send().await?;
                }
                Ok(Command::Subscribe(subscription)) => {
                    match state.insert(&subscription.address, subscription.direction, chat_id) {
                        Ok(_) => {
                            cx.reply_to(format!("Subscribed to:\n`{}`", subscription.address))
                                .parse_mode(ParseMode::MarkdownV2)
                                .send()
                                .await?;
                        }
                        Err(e) => {
                            log::error!("failed to subscribe: {:?}", e);
                            cx.reply_to("Unable to subscribe to this address")
                                .send()
                                .await?;
                        }
                    }
                }
                Ok(Command::Unsubscribe(subscription)) => {
                    match state.remove(&subscription.address, subscription.direction, chat_id) {
                        Ok(_) => {
                            cx.reply_to(format!("Unsubscribed from:\n`{}`", subscription.address))
                                .parse_mode(ParseMode::MarkdownV2)
                                .send()
                                .await?;
                        }
                        Err(e) => {
                            log::error!("failed to unsubscribe: {:?}", e);
                            cx.reply_to("Unable to unsubscribe from this address")
                                .send()
                                .await?;
                        }
                    }
                }
                Ok(Command::List) => {
                    let mut response = "Subscriptions:".to_owned();
                    for (workchain, addr, direction) in state.subscriptions(chat_id) {
                        response +=
                            &format!("\n`{}:{} - {}`", workchain, hex::encode(&addr), direction);
                    }

                    cx.reply_to(response)
                        .parse_mode(ParseMode::MarkdownV2)
                        .send()
                        .await?;
                }
                Err(e) => {
                    cx.reply_to(e.to_string())
                        .parse_mode(ParseMode::MarkdownV2)
                        .send()
                        .await?;
                }
            };

            ResponseResult::Ok(())
        }
    })
    .await;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger();
    run().await
}
