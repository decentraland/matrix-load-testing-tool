use std::cmp::max;
use std::collections::HashSet;
use std::sync::Arc;

use crate::client::{Client, RegisterResult};
use crate::client::{LoginResult, SyncResult};
use crate::configuration::Config;
use crate::events::{SyncEvent, SyncEventsSender, UserNotifications, UserNotificationsSender};
use crate::room::RoomType;
use crate::simulation::Context;
use crate::text::get_random_string;
use async_channel::Sender;
use futures::lock::Mutex;
use matrix_sdk::locks::RwLock;
use matrix_sdk::ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};
use rand::distributions::Alphanumeric;
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::Rng;

#[derive(Clone, Debug)]
pub struct User {
    pub localpart: String,
    client: Client,
    pub state: State,
}

#[derive(Debug)]
enum SocialAction {
    AddFriend,
    SendMessage(RoomType),
    LogOut,
    UpdateStatus,
    CreateChannel,
    JoinChannel,
    GetChannelMembers,
    LeaveChannel,
    None,
}

#[derive(Clone, Debug)]
pub enum State {
    Unauthenticated,
    Unregistered,
    LoggedIn,
    Sync {
        rooms: Arc<RwLock<HashSet<(OwnedRoomId, RoomType)>>>, // rooms can be channels or direct messages
        events: Arc<Mutex<Vec<SyncEvent>>>, // recent events to be processed and react, for instance to respond to friends or join rooms
        cancel_sync: Sender<bool>,          // cancel sync task
        ticks_to_live: usize,               // ticks to live
    },
    LoggedOut,
}

impl User {
    pub async fn new(id_number: usize, notifier: SyncEventsSender, config: &Config) -> Self {
        let localpart = get_user_id_localpart(id_number, &config.simulation.execution_id);

        let client = Client::new(notifier, config).await;
        Self {
            localpart,
            client,
            state: State::Unregistered,
        }
    }

    pub async fn act(&mut self, context: &Context) {
        match &self.state {
            State::Unregistered => self.register().await,
            State::Unauthenticated => self.log_in().await,
            State::LoggedIn => self.sync(&context.config, &context.user_notifier).await,
            State::Sync { .. } => self.socialize(context).await,
            State::LoggedOut => self.restart(&context.config).await,
        }
    }

    async fn add_room(&self, room: (OwnedRoomId, RoomType)) {
        if let State::Sync { rooms, .. } = &self.state {
            rooms.write().await.insert(room);
        }
    }

    async fn restart(&mut self, config: &Config) {
        log::debug!("user '{}' act => {}", self.localpart, "RESTART");
        self.client.reset(config).await;
        self.state = State::Unauthenticated;
    }

    async fn log_in(&mut self) {
        log::debug!("user '{}' act => {}", self.localpart, "LOG IN");

        match self.client.login(&self.localpart).await {
            LoginResult::Ok => {
                self.state = State::LoggedIn;
            }
            LoginResult::NotRegistered => {
                log::debug!("user {} not registered", self.localpart);
                self.state = State::Unregistered;
            }
            LoginResult::Failed => {
                log::debug!(
                    "user {} failed to login, maybe retry next time...",
                    self.localpart
                );
            }
        }
    }

    async fn register(&mut self) {
        log::debug!("user '{}' act => {}", self.localpart, "REGISTER");
        match self.client.register(&self.localpart).await {
            RegisterResult::Ok => self.state = State::Unauthenticated,
            RegisterResult::Failed => log::debug!(
                "could not register user {}, will retry next time...",
                self.localpart
            ),
        }
    }

    pub fn id(&self) -> Option<&UserId> {
        self.client.user_id()
    }

    pub fn get_user_channels_stats<'a>(
        &'a self,
        (max, min, total_chans_joined_by_users, channels_created): (
            usize,
            usize,
            usize,
            &'a mut HashSet<OwnedRoomId>,
        ),
    ) -> (usize, usize, usize, &HashSet<OwnedRoomId>) {
        let mut current_max: usize = max;
        let mut current_min: usize = min;
        let mut current_total: usize = total_chans_joined_by_users;

        if let State::Sync { rooms, .. } = &self.state {
            let rooms = rooms.try_read();
            if let Ok(rooms) = rooms {
                if !rooms.is_empty() {
                    let rooms_channels = rooms
                        .iter()
                        .filter(|room| matches!(room.1, RoomType::Channel))
                        .collect::<Vec<_>>();

                    let current_chans = rooms_channels.len();
                    log::debug!(
                        "get_user_channels_stats: current chans for user {}:  {}",
                        self.localpart,
                        current_chans
                    );
                    if current_chans > max {
                        current_max = current_chans
                    }

                    if current_chans < min {
                        current_min = current_chans
                    }

                    current_total += current_chans;

                    for room in rooms_channels {
                        channels_created.insert(room.0.clone());
                    }
                }
            }
        }
        (current_max, current_min, current_total, channels_created)
    }

    async fn sync(&mut self, config: &Config, user_notifier: &UserNotificationsSender) {
        log::debug!("user '{}' act => {}", self.localpart, "SYNC");
        match self
            .client
            .sync(user_notifier, config.feature_flags.presence_enabled)
            .await
        {
            SyncResult::Ok {
                rooms,
                invited_rooms,
                cancel_sync,
            } => {
                log::debug!(
                    "user '{}' has {} dm rooms",
                    self.localpart,
                    get_room_count(&rooms, RoomType::DirectMessage)
                );
                log::debug!(
                    "user '{}' has {} channels",
                    self.localpart,
                    get_room_count(&rooms, RoomType::Channel)
                );
                log::debug!(
                    "user '{}' has been invited to {} rooms",
                    self.localpart,
                    &invited_rooms.len()
                );

                let mut events = vec![];
                for invited_room in invited_rooms {
                    events.push(SyncEvent::Invite(invited_room));
                }

                for (room_id, _) in &rooms {
                    events.push(SyncEvent::UnreadRoom(room_id.clone()));
                }

                let rooms = rooms
                    .iter()
                    .fold(HashSet::new(), |mut set, (room_id, room_type)| {
                        set.insert((room_id.to_owned(), room_type.clone()));
                        set
                    });

                let ticks_to_live = get_ticks_to_live(config);
                self.state = State::Sync {
                    rooms: Arc::new(RwLock::new(rooms)),
                    events: Arc::new(Mutex::new(events)),
                    cancel_sync,
                    ticks_to_live,
                };
                let user_id = self.id();
                if let Some(user_id) = user_id {
                    user_notifier
                        .send(UserNotifications::NewSyncedUser(user_id.to_owned()))
                        .await
                        .expect("channel to be open");

                    log::debug!(
                        "user '{}' with id {} sent to collector",
                        self.localpart,
                        user_id
                    );
                } else {
                    log::debug!("user '{}' doesn't have user_id to send", self.localpart);
                }
                log::debug!("user '{}' now is syncing", self.localpart);
            }
            SyncResult::Failed => log::debug!(
                "user {} couldn't make initial sync, will retry next time...",
                self.localpart
            ),
        }
    }

    async fn read_sync_events(&self, events: &Mutex<Vec<SyncEvent>>) {
        log::debug!("user '{}' reading sync events", self.localpart);
        let new_events = self.client.read_sync_events().await;
        let mut events = events.lock().await;
        for event in new_events {
            match event {
                SyncEvent::RoomCreated(room_id) => {
                    self.add_room((room_id.to_owned(), RoomType::DirectMessage))
                        .await
                }
                SyncEvent::ChannelCreated(room_id) => {
                    self.add_room((room_id.to_owned(), RoomType::Channel)).await
                }
                _ => events.push(event),
            }
        }
    }

    // user social skills are:
    // - react to received messages or invitations
    // - send a message to a friend
    // - add a new friend
    // - update status
    // - log out (not so social)
    async fn socialize(&mut self, context: &Context) {
        log::debug!("user '{}' act => {}", self.localpart, "SOCIALIZE");

        self.decrease_ticks_to_live();
        if let State::Sync {
            rooms,
            events,
            cancel_sync,
            ticks_to_live,
        } = &self.state
        {
            self.read_sync_events(events).await;
            let mut events = events.lock().await;
            if let Some(event) = events.pop() {
                log::debug!("--- user '{}' going to react", self.localpart);
                self.react(event, context).await
            } else {
                drop(events);

                log::debug!("--- user '{}' going to start interaction", self.localpart);
                if ticks_to_live <= &0 {
                    // it's time to log out
                    self.log_out(cancel_sync.clone(), &context.user_notifier)
                        .await;
                } else {
                    match pick_random_action(
                        context.config.simulation.probability_to_act,
                        context.config.feature_flags.channels_load,
                        context.config.feature_flags.allow_get_channel_members,
                    ) {
                        SocialAction::SendMessage(message_type) => match message_type {
                            RoomType::DirectMessage => {
                                self.send_message(
                                    pick_room(rooms, RoomType::DirectMessage).await,
                                    message_type,
                                )
                                .await
                            }
                            RoomType::Channel => {
                                self.send_message(
                                    pick_room(rooms, RoomType::Channel).await,
                                    message_type,
                                )
                                .await
                            }
                        },
                        SocialAction::AddFriend => self.add_friend(context).await,
                        SocialAction::LogOut => {
                            self.log_out(cancel_sync.clone(), &context.user_notifier)
                                .await
                        }
                        SocialAction::UpdateStatus => self.update_status().await,
                        SocialAction::CreateChannel => {
                            let rooms = rooms.read().await;
                            self.create_channel(
                                get_room_count(&*rooms, RoomType::Channel),
                                context.config.simulation.channels_per_user,
                            )
                            .await
                        }
                        SocialAction::JoinChannel => {
                            self.join_channel(self.pick_channel(context).await, context)
                                .await
                        }
                        SocialAction::GetChannelMembers => {
                            let channel_id = pick_room(rooms, RoomType::Channel).await;
                            if let Some(channel_id) = channel_id {
                                self.get_channel_members(
                                    channel_id,
                                    SocialAction::GetChannelMembers,
                                )
                                .await;
                            }
                        }
                        SocialAction::LeaveChannel => {
                            self.leave_channel(pick_room(rooms, RoomType::Channel).await)
                                .await
                        }
                        SocialAction::None => log::debug!("user {} did nothing", self.localpart),
                    };
                }
            }
        } else {
            log::debug!("user cannot socialize if is not in sync state!");
        }
    }

    fn decrease_ticks_to_live(&mut self) {
        if let State::Sync { ticks_to_live, .. } = &mut self.state {
            *ticks_to_live -= 1;
        }
    }
    async fn react(&self, event: SyncEvent, ctx: &Context) {
        log::debug!("user '{}' act => {}", self.localpart, "REACT");
        match event {
            SyncEvent::Invite(room_id) => self.join(&room_id, RoomType::DirectMessage, false).await,
            SyncEvent::MessageReceived(room_id, _, message_type) => {
                if RoomType::Channel == message_type && !ctx.config.feature_flags.channels_load {
                    log::debug!(
                        "user '{}' not responding because channels are disabled",
                        self.localpart
                    );
                    return;
                }
                self.respond(room_id, message_type).await
            }
            SyncEvent::UnreadRoom(room_id) => self.read_messages(room_id).await,
            SyncEvent::GetChannelMembers(room_id) => {
                self.get_channel_members(room_id, SocialAction::JoinChannel)
                    .await
            }
            _ => {}
        }
    }

    async fn read_messages(&self, room_id: OwnedRoomId) {
        log::debug!("user '{}' act => {}", self.localpart, "READ MESSAGES");
        self.client.read_messages(room_id).await;
    }

    async fn get_channel_members(&self, room_id: OwnedRoomId, social_action: SocialAction) {
        log::debug!(
            "user '{}' act => GET CHANNEL MEMBERS BY {:?}",
            self.localpart,
            social_action
        );
        self.client.get_channel_members(&room_id).await
    }

    async fn respond(&self, room: OwnedRoomId, message_type: RoomType) {
        match message_type {
            RoomType::DirectMessage => log::debug!(
                "user '{}' act => {}",
                self.localpart,
                "RESPOND DIRECT MESSAGE"
            ),
            RoomType::Channel => {
                log::debug!("user '{}' act => {}", self.localpart, "RESPOND CHANNEL")
            }
        }
        self.send_message(Some(room), message_type).await;
    }

    async fn add_friend(&self, context: &Context) {
        log::debug!("user '{}' act => {}", self.localpart, "ADD FRIEND");
        let friend_id = self.pick_friend(context).await;
        if let Some(friend_id) = friend_id {
            self.client.add_friend(&friend_id).await;
        } else {
            log::debug!("there are no users to add as friend :(");
        }
    }

    async fn create_channel(&self, current_user_channels: usize, channels_per_user: usize) {
        if current_user_channels < channels_per_user {
            let channel_name: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect();
            log::debug!(
                "user '{}' act => {} => {}",
                self.localpart,
                "CREATE CHANNEL",
                channel_name
            );
            self.client.create_channel(channel_name).await
        } else {
            log::debug!(
                "user '{}' act => {} per user: {}, current user: {}",
                self.localpart,
                "REACH CHANNEL LIMIT CREATION",
                channels_per_user,
                current_user_channels
            )
        }
    }

    async fn join_channel(&self, room_id: Option<OwnedRoomId>, context: &Context) {
        if let Some(room_id) = room_id {
            self.join(
                &room_id,
                RoomType::Channel,
                context.config.feature_flags.allow_get_channel_members,
            )
            .await;
        } else {
            log::debug!("user {} has no room to join", self.localpart);
        }
    }

    async fn pick_channel(&self, context: &Context) -> Option<OwnedRoomId> {
        let room_type = RoomType::Channel;
        let user_channels = match &self.state {
            State::Sync { rooms, .. } => rooms
                .read()
                .await
                .iter()
                .filter(|(_, r)| room_type == *r)
                .fold(HashSet::new(), |mut channels, item| {
                    channels.insert(item.0.to_owned());
                    channels
                }),
            _ => {
                log::debug!("user '{}' was not synced", self.localpart);
                return None;
            }
        };
        if user_channels.len() < context.config.simulation.channels_per_user {
            log::debug!("user {} reach the channels per user limit", self.localpart);
            return None;
        }

        // we use this type of Random because it can be used in threads
        // check out: https://docs.rs/rand/0.5.0/rand/rngs/struct.StdRng.html
        // or https://stackoverflow.com/questions/65053037/how-can-i-clone-a-random-number-generator-for-different-threads-in-rust
        let mut rng: StdRng = rand::SeedableRng::from_entropy();

        let ctx_channels = context.channels.read().await;

        let exclude_user_channels = ctx_channels.difference(&user_channels).collect::<Vec<_>>();

        exclude_user_channels
            .choose(&mut rng)
            .map(|r| (*r).to_owned())
    }

    async fn leave_channel(&self, channel_id: Option<OwnedRoomId>) {
        log::debug!("user '{}' act => {}", self.localpart, "LEAVE CHANNEL");
        match channel_id {
            Some(room_id) => {
                log::debug!("channel about to leave: {room_id}");
                self.client.leave_room(room_id).await
            }
            None => log::debug!("there is no room to leave"),
        }
    }

    async fn join(&self, room: &RoomId, room_type: RoomType, allow_get_channel_members: bool) {
        log::debug!("user '{}' act => JOIN {:?}", self.localpart, room_type);

        self.client
            .join_room(room, room_type, allow_get_channel_members)
            .await;
    }

    async fn send_message(&self, room: Option<OwnedRoomId>, message_type: RoomType) {
        log::debug!(
            "user '{}' act => SEND {:?} MESSAGE",
            self.localpart,
            message_type
        );
        if let Some(room) = room {
            self.client.send_message(&room, get_random_string()).await;
        } else {
            log::debug!(
                "trying to send message to {:?} but don't have one :(",
                message_type
            )
        }
    }

    /// Log out user and append new char to the localpart string so next iteration is a new user.
    async fn log_out(
        &mut self,
        cancel_sync: Sender<bool>,
        user_notifier: &UserNotificationsSender,
    ) {
        log::debug!("user '{}' act => {}", self.localpart, "LOG OUT");
        cancel_sync.send(true).await.expect("channel open");
        self.state = State::LoggedOut;
        self.localpart += "_";
        let user_id = self.id();
        if let Some(user_id) = user_id {
            user_notifier
                .send(UserNotifications::UserLoggedOut(user_id.to_owned()))
                .await
                .expect("channel to be open");
        } else {
            log::debug!(
                "user '{}' doesn't have user_id to send to log out",
                self.localpart
            );
        }
    }

    async fn update_status(&self) {
        log::debug!("user '{}' act => {}", self.localpart, "UPDATE STATUS");
        self.client.update_status().await;
    }

    async fn pick_friend(&self, context: &Context) -> Option<OwnedUserId> {
        let mut rng: StdRng = rand::SeedableRng::from_entropy(); // allow use it with threads
        let synced_users = context.syncing_users.read().await;

        let mut synced_users = synced_users.iter().collect::<Vec<_>>();
        synced_users.shuffle(&mut rng);

        while let Some(friend_id) = synced_users.pop() {
            if friend_id.localpart() != self.localpart {
                return Some(friend_id.to_owned());
            }
        }
        None
    }
}

fn get_room_count<'r, I>(rooms: I, room_type: RoomType) -> usize
where
    I: IntoIterator<Item = &'r (OwnedRoomId, RoomType)>,
{
    rooms.into_iter().filter(|(_, r)| room_type == *r).count()
}

fn get_user_id_localpart(id_number: usize, execution_id: &str) -> String {
    format!("user_{id_number}_{execution_id}")
}

// we probably want to distribute these actions and don't make them random (more send messages than logouts)
fn pick_random_action(
    probability_to_act: usize,
    channels_enabled: bool,
    allow_get_channel_members: bool,
) -> SocialAction {
    let mut rng = rand::thread_rng();
    if rng.gen_ratio(probability_to_act as u32, 100) {
        if rng.gen_ratio(1, 5) {
            SocialAction::SendMessage(RoomType::DirectMessage)
        } else if channels_enabled && rng.gen_ratio(1, 5) {
            SocialAction::SendMessage(RoomType::Channel)
        } else if rng.gen_ratio(1, 3) {
            SocialAction::AddFriend
        } else if rng.gen_ratio(1, 75) {
            SocialAction::LogOut
        } else if channels_enabled && rng.gen_ratio(1, 70) {
            SocialAction::LeaveChannel
        } else if channels_enabled && allow_get_channel_members && rng.gen_ratio(1, 60) {
            SocialAction::GetChannelMembers
        } else if channels_enabled && rng.gen_ratio(1, 50) {
            SocialAction::CreateChannel
        } else if channels_enabled && rng.gen_ratio(1, 35) {
            SocialAction::JoinChannel
        } else if rng.gen_ratio(1, 25) {
            SocialAction::UpdateStatus
        } else {
            SocialAction::SendMessage(RoomType::DirectMessage)
        }
    } else {
        SocialAction::None
    }
}

async fn pick_room(
    rooms: &RwLock<HashSet<(OwnedRoomId, RoomType)>>,
    room_type: RoomType,
) -> Option<OwnedRoomId> {
    rooms
        .read()
        .await
        .iter()
        .filter(|(_, r)| room_type == *r)
        .choose(&mut rand::thread_rng())
        .map(|room| room.0.to_owned())
}

/// Get random value for ticks to live related to the total of ticks in simulation,
/// so users can be short or long lived.
fn get_ticks_to_live(config: &Config) -> usize {
    let mut rng = rand::thread_rng();
    let short_lived = rng.gen_bool(config.simulation.probability_for_short_lifes as f64 / 100.);
    match short_lived {
        true => max(config.simulation.ticks / 100, 5),
        false => max(config.simulation.ticks / 10, 10),
    }
}
