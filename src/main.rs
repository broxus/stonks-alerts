mod listener;
mod settings;
mod state;

use std::sync::Arc;

use settings::Settings;
use teloxide::prelude::*;
use teloxide::types::ParseMode;
use teloxide::utils::command::BotCommand;

use crate::listener::*;
use crate::state::*;

const BOT_NAME: &str = "FreeTON Alerts";

#[derive(BotCommand)]
#[command(rename = "lowercase", description = "These commands are supported:")]
enum Command {
    #[command(description = "display this text")]
    Start,
    #[command(description = "subscribe to account", parse_with = "split")]
    Subscribe {
        direction: Direction,
        address: String,
    },
    #[command(description = "unsubscribe from account", parse_with = "split")]
    Unsubscribe {
        direction: Direction,
        address: String,
    },
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
    let state = Arc::new(State::new(settings.db)?);

    spawn_listener(settings.kafka, bot.clone(), state.clone())?;

    teloxide::commands_repl(bot, BOT_NAME, move |cx, command| {
        let state = state.clone();

        async move {
            let chat_id = cx.update.chat_id();

            match command {
                Command::Start => {
                    cx.answer(Command::descriptions()).send().await?;
                }
                Command::Subscribe { direction, address } => {
                    match state.insert(&address, direction, chat_id) {
                        Ok(_) => {
                            cx.reply_to(format!("Subscribed to:\n`{}`", address))
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
                Command::Unsubscribe { direction, address } => {
                    match state.remove(&address, direction, chat_id) {
                        Ok(_) => {
                            cx.reply_to(format!("Unsubscribed from:\n`{}`", address))
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
                Command::List => {
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
