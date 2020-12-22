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

fn parse_filter<'a, I>(mut tokens: I) -> Result<SubscriptionFilter, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    let value_token = match tokens.next() {
        Some(token) if token == ">" || token == ">=" => tokens.next(),
        Some(_) => {
            return Err(ParseError::Custom(
                format!("unsupported filter, possible patterns are: `> AMOUNT`").into(),
            ))
        }
        None => return Ok(SubscriptionFilter::default()),
    };

    match value_token {
        Some(token) => match f64::from_str(token) {
            Ok(gt) => Ok(SubscriptionFilter {
                gt: Some((gt * 1000000000.0) as u64),
                ..Default::default()
            }),
            Err(e) => Err(ParseError::Custom(e.to_string().into())),
        },
        None => Ok(SubscriptionFilter::default()),
    }
}

struct Subscription {
    direction: Direction,
    address: String,
    filter: SubscriptionFilter,
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
                    filter: parse_filter(parts)?,
                },)),
                Err(e) => Err(ParseError::IncorrectFormat(
                    format!("{}\n\n{}", e.to_string(), description).into(),
                )),
            },
            None => Ok((Subscription {
                direction: Direction::All,
                address: first.to_owned(),
                filter: Default::default(),
            },)),
        },
        None => Err(ParseError::Custom(description.to_owned().into())),
    }
}

struct SetComment {
    address: String,
    comment: String,
}

const ESCAPED_CHARACTERS: [char; 18] = [
    '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!',
];

const ESCAPED_CHARACTERS_REPLACEMENT: [&str; 18] = [
    "\\_", "\\*", "\\[", "\\]", "\\(", "\\)", "\\~", "\\`", "\\>", "\\#", "\\+", "\\-", "\\=",
    "\\|", "\\{", "\\}", "\\.", "\\!",
];

fn parse_set_comment(input: String) -> Result<(SetComment,), ParseError> {
    fn escape_markdown(text: &str) -> String {
        let mut text = text.to_string();
        for (character, replacement) in ESCAPED_CHARACTERS
            .iter()
            .zip(ESCAPED_CHARACTERS_REPLACEMENT.iter())
        {
            text = text.replace(*character, replacement);
        }
        text
    }

    let description = "Usage:\n    `/setcomment ADDRESS COMMENT`\n\nWhere:\n    `ADDRESS` \\- raw or packed address\n    `COMMENT` \\- some notes, 40 characters max";

    let input = input.trim();
    match input.find(' ') {
        Some(space_pos) => {
            let (address, comment) = input.split_at(space_pos);

            let comment = {
                let trimmed_comment = comment.trim();
                if trimmed_comment.len() > 40 {
                    escape_markdown(&trimmed_comment[0..40]) + "\\.\\.\\."
                } else {
                    escape_markdown(trimmed_comment)
                }
            };

            Ok((SetComment {
                address: address.to_string(),
                comment,
            },))
        }
        None => Err(ParseError::Custom(description.into())),
    }
}

struct ClearComment {
    address: String,
}

fn parse_clear_comment(input: String) -> Result<(ClearComment,), ParseError> {
    Ok((ClearComment {
        address: input.trim().to_string(),
    },))
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
    #[command(description = "set address comment", parse_with = "parse_set_comment")]
    SetComment(SetComment),
    #[command(
        description = "clear address comment",
        parse_with = "parse_clear_comment"
    )]
    ClearComment(ClearComment),
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
                    match state.insert(
                        &subscription.address,
                        subscription.direction,
                        subscription.filter,
                        chat_id,
                    ) {
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
                Ok(Command::SetComment(data)) => {
                    match state.set_comment(chat_id, &data.address, &data.comment) {
                        Ok(_) => {
                            cx.reply_to("Done").send().await?;
                        }
                        Err(e) => {
                            log::error!("failed to set comment: {:?}", e);
                            cx.reply_to("Failed to set comment for this address")
                                .send()
                                .await?;
                        }
                    }
                }
                Ok(Command::ClearComment(data)) => {
                    match state.remove_comment(chat_id, &data.address) {
                        Ok(_) => {
                            cx.reply_to("Done").send().await?;
                        }
                        Err(e) => {
                            log::error!("failed to clear comment: {:?}", e);
                            cx.reply_to("Failed to clear comment for this address")
                                .send()
                                .await?;
                        }
                    }
                }
                Ok(Command::List) => {
                    let mut response = "Subscriptions:".to_owned();
                    for (i, (workchain, addr, direction, filter)) in
                        state.subscriptions(chat_id).enumerate()
                    {
                        let comment = state
                            .get_comment(chat_id, workchain, &addr)
                            .unwrap_or_default()
                            .unwrap_or_else(|| format!("{}\\. Unknown", i));

                        response += &format!(
                            "\n\n{} \\- {} {}\n`{}:{}`",
                            comment,
                            direction,
                            filter,
                            workchain,
                            hex::encode(&addr),
                        );
                    }

                    if let Err(e) = cx
                        .reply_to(response)
                        .parse_mode(ParseMode::MarkdownV2)
                        .send()
                        .await
                    {
                        log::error!("{}", e);
                    }
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
