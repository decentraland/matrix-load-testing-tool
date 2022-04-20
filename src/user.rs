use crate::events::Event;
use crate::text::get_random_string;
use indicatif::ProgressBar;
use crate::text::{create_progress_bar, get_random_string};
use crate::time::time_now;
use crate::users_state::{load_users, save_users, SavedUserState};
use crate::Configuration;
use futures::StreamExt;
use indicatif::ProgressBar;
use matrix_sdk::config::RequestConfig;
use matrix_sdk::room::Room;
use matrix_sdk::ruma::api::client::uiaa::{AuthData, Dummy, UiaaResponse};
use matrix_sdk::ruma::api::error::FromHttpResponseError::Server;
use matrix_sdk::ruma::api::error::ServerError::Known;
use matrix_sdk::ruma::{
    api::client::{
        account::register::v3::Request as RegistrationRequest, error::ErrorKind,
        room::create_room::v3::Request as CreateRoomRequest,
    },
    assign,
};
use matrix_sdk::ruma::{RoomId, UserId};
use matrix_sdk::Client;
use matrix_sdk::HttpError::UiaaError;
use matrix_sdk::{
    config::SyncSettings,
    ruma::events::{
        room::message::{OriginalSyncRoomMessageEvent, RoomMessageEventContent},
        AnyMessageLikeEventContent,
    },
};
use rand::Rng;
use regex::Regex;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use strum::Display;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

const PASSWORD: &str = "asdfasdf";

pub struct Disconnected {
    retry_enabled: bool,
    respect_login_well_known: bool,
}
pub struct Registered;
pub struct LoggedIn;
#[derive(Clone)]
pub struct Synching {
    rooms: Arc<Mutex<Vec<Box<RoomId>>>>,
}

#[derive(Clone)]
pub struct User<State> {
    id: Box<UserId>,
    client: Arc<Mutex<Client>>,
    tx: Sender<Event>,
    state: State,
}

impl<State> User<State> {
    pub fn id(&self) -> &UserId {
        self.id.as_ref()
    }

    pub async fn send(&self, event: Event) {
        log::info!("Sending event {:?}", event);
        if self.tx.send(event).await.is_err() {
            log::info!("Receiver dropped");
        }
    }
}

#[derive(Serialize, Debug, Eq, Hash, PartialEq, Clone, Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum UserRequest {
    Register,
    Login,
    CreateRoom,
    JoinRoom,
    SendMessage,
}

impl User<Disconnected> {
    pub async fn new(
        id: &str,
        homeserver: &str,
        retry_enabled: bool,
        respect_login_well_known: bool,
        tx: Sender<Event>,
    ) -> Option<User<Disconnected>> {
        // TODO: check which protocol we want to use: http or https (defaulting to https)
        let (homeserver_no_protocol, homeserver_url) = get_homeserver_url(homeserver, None);

        let user_id = UserId::parse(format!("@{id}:{homeserver_no_protocol}").as_str()).unwrap();

        let instant = Instant::now();

        log::info!("Attempt to create a client with id {}", user_id);

        let timeout = Duration::from_secs(30);
        let request_config = if retry_enabled {
            RequestConfig::new().retry_timeout(timeout)
        } else {
            RequestConfig::new().disable_retry().timeout(timeout)
        };

        let client = Client::builder()
            .request_config(request_config)
            .homeserver_url(homeserver_url)
            .respect_login_well_known(respect_login_well_known)
            .build()
            .await;
        if client.is_err() {
            log::info!("Failed to create client");
            return None;
        }

        log::info!(
            "New client created {} {}",
            user_id,
            instant.elapsed().as_millis()
        );

        Some(Self {
            id: user_id,
            client: Arc::new(Mutex::new(client.unwrap())),
            tx,
            state: Disconnected {
                retry_enabled,
                respect_login_well_known,
            },
        })
    }

    pub async fn register(&mut self) -> Option<User<Registered>> {
        let instant = Instant::now();

        let req = assign!(RegistrationRequest::new(), {
            username: Some(self.id.localpart()),
            password: Some(PASSWORD),
            auth: Some(AuthData::Dummy(Dummy::new()))
        });
        let client = self.client.lock().await;
        let response = client.register(req).await;

        match response {
            Ok(_) => {
                self.send(Event::RequestDuration((
                    UserRequest::Register,
                    instant.elapsed(),
                )))
                .await;
                Some(User {
                    id: self.id.clone(),
                    client: self.client.clone(),
                    tx: self.tx.clone(),
                    state: Registered {},
                })
            }
            Err(e) => {
                // if ID is already taken, proceed as Registered
                if let UiaaError(Server(Known(UiaaResponse::MatrixError(e)))) = e {
                    if e.kind == ErrorKind::UserInUse {
                        log::info!("Client already registered, proceed to Login {}", self.id());
                        let user = User::new(
                            self.id.localpart(),
                            self.id.server_name().as_str(),
                            self.state.retry_enabled,
                            self.state.respect_login_well_known,
                            self.tx.clone(),
                        )
                        .await
                        .unwrap();
                        return Some(User {
                            id: user.id,
                            client: user.client,
                            tx: user.tx,
                            state: Registered {},
                        });
                    }
                } else {
                    self.send(Event::Error((UserRequest::Register, e))).await;
                }
                None
            }
        }
    }
}

impl User<Registered> {
    pub async fn login(&mut self) -> Option<User<LoggedIn>> {
        let instant = Instant::now();

        let client = self.client.lock().await;
        log::info!("Attempt to login client with id {}", self.id());
        let response = client
            .login(self.id.localpart(), PASSWORD, None, None)
            .await;

        log::info!("Login response: {:?}", response);
        match response {
            Ok(_) => {
                self.send(Event::RequestDuration((
                    UserRequest::Login,
                    instant.elapsed(),
                )))
                .await;
                Some(User {
                    id: self.id.clone(),
                    client: self.client.clone(),
                    tx: self.tx.clone(),
                    state: LoggedIn {},
                })
            }
            Err(e) => {
                if let matrix_sdk::Error::Http(e) = e {
                    self.send(Event::Error((UserRequest::Login, e))).await;
                }

                None
            }
        }
    }
}

impl User<LoggedIn> {
    pub async fn sync(&self) -> User<Synching> {
        let client = self.client.lock().await;
        client
            .register_event_handler({
                let tx = self.tx.clone();
                let user_id = self.id.clone();
                move |ev, room| {
                    let tx = tx.clone();
                    let user_id = user_id.clone();
                    async move {
                        on_room_message(ev, room, tx, user_id).await;
                    }
                }
            })
            .await;

        tokio::spawn({
            // we are not cloning the mutex to avoid locking it forever
            let client = client.clone();
            async move {
                client.sync(SyncSettings::default()).await;
            }
        });

        User {
            id: self.id.clone(),
            client: self.client.clone(),
            tx: self.tx.clone(),
            state: Synching {
                rooms: Arc::new(Mutex::new(vec![])),
            },
        }
    }
}

impl User<Synching> {
    pub async fn create_room(&mut self) -> Option<Box<RoomId>> {
        let client = self.client.lock().await;

        let instant = Instant::now();
        let request = CreateRoomRequest::new();
        let response = client.create_room(request).await;
        match response {
            Ok(ref response) => {
                self.send(Event::RequestDuration((
                    UserRequest::CreateRoom,
                    instant.elapsed(),
                )))
                .await;
                Some(response.room_id.clone())
            }
            Err(e) => {
                self.send(Event::Error((UserRequest::CreateRoom, e))).await;
                None
            }
        }
    }

    pub async fn join_room(&mut self, room_id: &RoomId) {
        let client = self.client.lock().await;
        let instant = Instant::now();
        let response = client.join_room_by_id(room_id).await;
        match response {
            Ok(ref response) => {
                self.send(Event::RequestDuration((
                    UserRequest::JoinRoom,
                    instant.elapsed(),
                )))
                .await;
                self.state.rooms.lock().await.push(response.room_id.clone());
            }
            Err(e) => {
                self.send(Event::Error((UserRequest::JoinRoom, e))).await;
            }
        }
    }

    pub async fn act(&mut self) {
        let client = self.client.lock().await;
        let rooms = self.state.rooms.lock().await;

        if rooms.len() == 0 {
            return;
        }

        let room_id = &rooms[rand::thread_rng().gen_range(0..rooms.len())];
        let content = AnyMessageLikeEventContent::RoomMessage(RoomMessageEventContent::text_plain(
            get_random_string(),
        ));
        let instant = Instant::now();

        if let Some(room) = client.get_joined_room(room_id) {
            let response = room.send(content, None).await;
            match response {
                Ok(response) => {
                    self.send(Event::RequestDuration((
                        UserRequest::SendMessage,
                        instant.elapsed(),
                    )))
                    .await;

                    self.send(Event::MessageSent(response.event_id.to_string()))
                        .await;
                }
                Err(e) => {
                    if let matrix_sdk::Error::Http(e) = e {
                        self.send(Event::Error((UserRequest::SendMessage, e))).await;
                    }
                }
            }
        } else {
            // TODO! check why this can be possible
        }
    }
}

pub fn join_users_to_room(
    first_user: &User<Synching>,
    second_user: &User<Synching>,
    progress_bar: &ProgressBar,
) -> impl futures::Future<Output = ()> {
    let mut first_user = first_user.clone();
    let mut second_user = second_user.clone();
    let progress_bar = progress_bar.clone();

    async move {
        let room_created = first_user.create_room().await;
        if let Some(room_id) = room_created {
            first_user.join_room(&room_id).await;
            second_user.join_room(&room_id).await;
        } else {
            //TODO!: This should panic or abort somehow after exhausting all retries of creating the room
            log::info!("User {} couldn't create a room", first_user.id());
        }
        progress_bar.inc(1);
    }
}

pub fn create_user<'a>(
    id: String,
    server: &'a str,
    progress_bar: &'a ProgressBar,
    tx: Sender<Event>,
    retry_attempts: usize,
    retry_enabled: bool,
    respect_login_well_known: bool,
) -> impl futures::Future<Output = Option<User<Synching>>> + 'a {
    let progress_bar = progress_bar.clone();
    async move {
        let id = format!("user_{id}");
        for _ in 0..retry_attempts {
            let user = User::new(
                &id,
                server,
                retry_enabled,
                respect_login_well_known,
                tx.clone(),
            )
            .await;

            if let Some(mut user) = user {
                if let Some(mut user) = user.register().await {
                    if let Some(user) = user.login().await {
                        log::info!("User is now synching: {}", user.id());
                        progress_bar.inc(1);
                        return Some(user.sync().await);
                    }
                }
            }
        }

        //TODO!: This should panic or abort somehow after exhausting all retries of creating the user
        log::info!("Couldn't init a user");
        progress_bar.inc(1);
        None
    }
}

async fn on_room_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    sender: Sender<Event>,
    user_id: Box<UserId>,
) {
    if let Room::Joined(room) = room {
        if event.sender.localpart() == user_id.localpart() {
            return;
        }
        sender
            .send(Event::MessageReceived(event.event_id.to_string()))
            .await
            .expect("Receiver dropped");
        log::info!(
            "User {} received a message from room {} and sent by {}",
            user_id,
            room.room_id(),
            event.sender
        );
    }
}

/// This function returns homeserver domain and url, ex:
///  - get_homeserver_url("matrix.domain.com") => ("matrix.domain.com", "https://matrix.domain.com")
fn get_homeserver_url<'a>(homeserver: &'a str, protocol: Option<&'a str>) -> (&'a str, String) {
    let regex = Regex::new(r"https?://").unwrap();
    if regex.is_match(homeserver) {
        let parts: Vec<&str> = regex.splitn(homeserver, 2).collect();
        (parts[1], homeserver.to_string())
    } else {
        let protocol = protocol.unwrap_or("https");
        (homeserver, format!("{protocol}://{homeserver}"))
    }
}

async fn get_client(homeserver_url: String, retry_enabled: bool) -> Option<Client> {
    log::info!("Attempt to create a client with id ");

    let request_config = if retry_enabled {
        RequestConfig::new().retry_timeout(Duration::from_secs(30))
    } else {
        RequestConfig::new()
            .disable_retry()
            .timeout(Duration::from_secs(30))
    };

    let client = Client::builder()
        .request_config(request_config)
        .homeserver_url(homeserver_url)
        .build()
        .await;
    if client.is_err() {
        log::info!("Failed to create client");
        return None;
    }

    return Some(client.unwrap());
}

pub async fn create_desired_users(config: &Configuration) {
    let users_to_create = config.user_count;

    let timestamp = time_now();

    let mut users = vec![];
    let progress_bar = create_progress_bar(
        "Init users".to_string(),
        users_to_create.try_into().unwrap(),
    );
    progress_bar.tick();

    let homeserver_url = config.homeserver_url.clone();

    let mut client = get_client(homeserver_url.clone(), config.retry_request_config)
        .await
        .unwrap();

    let futures = (0..users_to_create).map(|i| {
        create_user(
            homeserver_url.clone(),
            &progress_bar,
            i,
            config.user_creation_retry_attempts,
            timestamp,
            config.retry_request_config,
            &client,
        )
    });

    let stream_iter = futures::stream::iter(futures);
    let mut buffered_iter = stream_iter.buffer_unordered(config.user_creation_throughput);

    while let Some(user) = buffered_iter.next().await {
        if let Some(user) = user {
            users.push(user);
        }
    }

    progress_bar.finish_and_clear();

    let mut current_users = load_users(config.users_filename.clone());

    current_users.add_user(
        timestamp,
        SavedUserState {
            homeserver_url: homeserver_url.clone(),
            amount: config.user_count,
            friendships: vec![],
        },
    );

    save_users(&current_users, config.users_filename.clone());
}

fn create_user(
    server: String,
    progress_bar: &ProgressBar,
    i: i64,
    retry_attempts: usize,
    timestamp: u128,
    retry_enabled: bool,
    client: &Client,
) -> impl futures::Future<Output = Option<User<Synching>>> {
    let progress_bar = progress_bar.clone();
    async move {
        let id = format!("user_{i}_{timestamp}");

        //TODO!: This should panic or abort somehow after exhausting all retries of creating the user
        log::info!("Couldn't init a user");
        progress_bar.inc(1);
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::user::*;
    #[test]
    fn homeserver_arg_can_start_with_https() {
        let homeserver_arg = "https://matrix.domain.com";
        assert_eq!(
            ("matrix.domain.com", homeserver_arg.to_string()),
            get_homeserver_url(homeserver_arg, None)
        );
    }

    #[test]
    fn homeserver_arg_can_start_with_http() {
        let homeserver_arg = "http://matrix.domain.com";

        assert_eq!(
            ("matrix.domain.com", homeserver_arg.to_string()),
            get_homeserver_url(homeserver_arg, None)
        );
    }

    #[test]
    fn homeserver_arg_can_start_without_protocol() {
        let homeserver_arg = "matrix.domain.com";
        let expected_homeserver_url = "https://matrix.domain.com";

        assert_eq!(
            (homeserver_arg, expected_homeserver_url.to_string()),
            get_homeserver_url(homeserver_arg, None)
        );
    }

    #[test]
    fn homeserver_should_return_specified_protocol() {
        let homeserver_arg = "matrix.domain.com";
        let expected_homeserver_url = "http://matrix.domain.com";

        assert_eq!(
            (homeserver_arg, expected_homeserver_url.to_string()),
            get_homeserver_url(homeserver_arg, Some("http"))
        );
    }
}
