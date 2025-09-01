use std::sync::Arc;

use crate::args;
mod groups;
mod identity;
mod messages;

pub use groups::*;
pub use identity::*;
pub use messages::*;

use color_eyre::eyre::Result;

#[derive(Debug)]
pub struct Generate {
    db: Arc<redb::Database>,
    opts: args::Generate,
    network: args::BackendOpts,
}

impl Generate {
    pub fn new(opts: args::Generate, network: args::BackendOpts, db: Arc<redb::Database>) -> Self {
        Self { opts, network, db }
    }

    pub async fn run(self) -> Result<()> {
        use args::EntityKind::*;
        let Generate { db, opts, network } = self;
        let args::Generate {
            entity,
            amount,
            invite,
            target_inbox,
            message_opts,
            concurrency,
        } = opts;

        info!(?concurrency, "using concurrency");

        match entity {
            Group => {
                GenerateGroups::new(db, network)
                    .create_groups(amount, invite.unwrap_or(0), *concurrency)
                    .await?;
                info!("groups generated");
                Ok(())
            }
            Message => {
                GenerateMessages::new(db, network, message_opts)
                    .run(amount, *concurrency)
                    .await?;
                info!("messages generated");
                Ok(())
            }
            Identity => {
                GenerateIdentity::new(db.into(), network)
                    .create_identities(amount, *concurrency)
                    .await?;
                info!("identities generated");
                Ok(())
            }
            Dm => {
                let target_inbox = target_inbox.ok_or_else(|| {
                    color_eyre::eyre::eyre!("target-inbox is required when generating DMs")
                })?;
                GenerateGroups::new(db, network)
                    .create_dms(amount, target_inbox, *concurrency)
                    .await?;
                info!("DMs generated");
                Ok(())
            }
        }
    }
}
