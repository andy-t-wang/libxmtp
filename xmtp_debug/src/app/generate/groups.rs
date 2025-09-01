//! Group Generation
use crate::app::identity_lock::get_identity_lock;
use crate::app::{
    store::{Database, GroupStore, IdentityStore, RandomDatabase},
    types::*,
};
use crate::{app, args};
use color_eyre::eyre::{self, ContextCompat, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;

pub struct GenerateGroups {
    group_store: GroupStore<'static>,
    identity_store: IdentityStore<'static>,
    // metadata_store: MetadataStore<'static>,
    network: args::BackendOpts,
}

impl GenerateGroups {
    pub fn new(db: Arc<redb::Database>, network: args::BackendOpts) -> Self {
        Self {
            group_store: db.clone().into(),
            identity_store: db.clone().into(),
            // metadata_store: db.clone().into(),
            network,
        }
    }

    #[allow(unused)]
    pub fn load_groups(&self) -> Result<Option<impl Iterator<Item = Result<Group>> + use<'_>>> {
        Ok(self
            .group_store
            .load(&self.network)?
            .map(|i| i.map(|i| Ok(i.value()))))
    }

    pub async fn create_groups(
        &self,
        n: usize,
        invitees: usize,
        concurrency: usize,
    ) -> Result<Vec<Group>> {
        // TODO: Check if identities still exist
        let mut groups: Vec<Group> = Vec::with_capacity(n);
        let style = ProgressStyle::with_template(
            "{bar} {pos}/{len} elapsed {elapsed} remaining {eta_precise}",
        );
        let bar = ProgressBar::new(n as u64).with_style(style.unwrap());
        let mut set: tokio::task::JoinSet<Result<_, eyre::Error>> = tokio::task::JoinSet::new();
        let mut handles = vec![];

        let network = &self.network;
        let mut rng = rand::thread_rng();

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        for _ in 0..n {
            let identity = self
                .identity_store
                .random(network, &mut rng)?
                .with_context(
                    || "no local identities found in database, have identities been generated?",
                )?;
            let invitees = self.identity_store.random_n(network, &mut rng, invitees)?;
            let bar_pointer = bar.clone();
            let network = network.clone();
            let semaphore = semaphore.clone();
            handles.push(set.spawn(async move {
                let _permit = semaphore.acquire().await?;
                let identity_lock = get_identity_lock(&identity.inbox_id)?;
                let _lock_guard = identity_lock.lock().await;

                debug!(address = identity.address(), "group owner");
                let client = app::client_from_identity(&identity, &network).await?;
                let ids = invitees
                    .iter()
                    .map(|i| hex::encode(i.inbox_id))
                    .collect::<Vec<_>>();
                let group = client.create_group(Default::default(), Default::default())?;
                
                // Sync the group to ensure it's ready before updating metadata
                if let Err(e) = group.sync().await {
                    warn!("Failed to sync group {}: {}", hex::encode(&group.group_id), e);
                }
                
                // Set the group name to a short timestamp for easy identification
                let group_name = format!("test-{}", std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs());
                
                println!("🏷️  SETTING GROUP NAME: {} for group {}", group_name, hex::encode(&group.group_id));
                if let Err(e) = group.update_group_name(group_name.clone()).await {
                    println!("❌ Failed to set group name to {}: {}", group_name, e);
                } else {
                    // Sync again multiple times to make sure the name update is fully processed
                    for i in 0..3 {
                        if let Err(e) = group.sync().await {
                            println!("⚠️  Failed to sync group after naming (attempt {}): {}", i+1, e);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    
                    // Verify the name was set correctly
                    match group.group_name() {
                        Ok(name) => {
                            if name.is_empty() {
                                println!("⚠️  Group {} name appears EMPTY after setting to '{}'", hex::encode(&group.group_id), group_name);
                            } else {
                                println!("✅ GROUP NAMED SUCCESSFULLY: '{}' for group {}", name, hex::encode(&group.group_id));
                            }
                        },
                        Err(e) => println!("❌ Failed to retrieve group name: {}", e),
                    }
                }
                
                group.add_members_by_inbox_id(ids.as_slice()).await?;
                bar_pointer.inc(1);
                let mut members = invitees
                    .into_iter()
                    .map(|i| i.inbox_id)
                    .collect::<Vec<InboxId>>();
                members.push(identity.inbox_id);
                Ok(Group {
                    id: group
                        .group_id
                        .try_into()
                        .expect("Group id expected to be 32 bytes"),
                    member_size: members.len() as u32,
                    members,
                    created_by: identity.inbox_id,
                })
            }));

            // going above 128 we hit "unable to open database errors"
            // This may be related to open file limits
            if set.len() >= 64
                && let Some(group) = set.join_next().await
            {
                match group {
                    Ok(group) => {
                        groups.push(group?);
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }
            }
        }

        while let Some(group) = set.join_next().await {
            match group {
                Ok(group) => {
                    groups.push(group?);
                }
                Err(e) => {
                    error!("{}", e.to_string());
                }
            }
        }
        self.group_store.set_all(groups.as_slice(), &self.network)?;
        Ok(groups)
    }

    pub async fn create_dms(
        &self,
        n: usize,
        target_inbox: args::InboxId,
        concurrency: usize,
    ) -> Result<Vec<Group>> {
        let mut dms: Vec<Group> = Vec::with_capacity(n);
        let style = ProgressStyle::with_template(
            "{bar} {pos}/{len} elapsed {elapsed} remaining {eta_precise}",
        );
        let bar = ProgressBar::new(n as u64).with_style(style.unwrap());
        let mut set: tokio::task::JoinSet<Result<_, eyre::Error>> = tokio::task::JoinSet::new();
        let mut handles = vec![];

        let network = &self.network;
        let mut rng = rand::thread_rng();

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        for _ in 0..n {
            let identity = self
                .identity_store
                .random(network, &mut rng)?
                .with_context(
                    || "no local identities found in database, have identities been generated?",
                )?;
            let bar_pointer = bar.clone();
            let network = network.clone();
            let target_inbox = target_inbox.clone();
            let semaphore = semaphore.clone();
            handles.push(set.spawn(async move {
                let _permit = semaphore.acquire().await?;
                let identity_lock = get_identity_lock(&identity.inbox_id)?;
                let _lock_guard = identity_lock.lock().await;

                debug!(address = identity.address(), target = %target_inbox, "creating DM");
                let client = app::client_from_identity(&identity, &network).await?;
                
                // Use find_or_create_dm_by_inbox_id to create a true DM conversation
                let dm = client.find_or_create_dm_by_inbox_id(target_inbox.to_string(), None).await?;
                
                bar_pointer.inc(1);
                let members = vec![identity.inbox_id.clone(), *target_inbox];
                
                Ok(Group {
                    id: dm
                        .group_id
                        .try_into()
                        .expect("DM group id expected to be 32 bytes"),
                    member_size: 2, // DMs always have exactly 2 members
                    members,
                    created_by: identity.inbox_id,
                })
            }));

            // going above 128 we hit "unable to open database errors"
            // This may be related to open file limits
            if set.len() >= 64
                && let Some(dm) = set.join_next().await
            {
                match dm {
                    Ok(dm) => {
                        dms.push(dm?);
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }
            }
        }

        while let Some(dm) = set.join_next().await {
            match dm {
                Ok(dm) => {
                    dms.push(dm?);
                }
                Err(e) => {
                    error!("{}", e.to_string());
                }
            }
        }
        self.group_store.set_all(dms.as_slice(), &self.network)?;
        info!("Created {} true DM conversations with target inbox {}", dms.len(), target_inbox);
        Ok(dms)
    }
}
