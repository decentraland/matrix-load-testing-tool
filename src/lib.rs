use friendship::{Friendship, FriendshipID};
use futures::future::join_all;
use futures::stream::iter;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use metrics::{Metrics, MetricsReport};
use rand::prelude::IteratorRandom;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DurationSeconds;
use std::fs::{create_dir_all, File};
use std::thread::sleep;
use std::time::{Duration, Instant};
use text::create_progress_bar;
use time::time_now;
use tokio::sync::mpsc::{self, Sender};
use tokio_context::task::TaskController;
use user::{create_user, join_users_to_room, Synching, User};

use crate::events::Event;

mod events;
mod friendship;
mod metrics;
mod text;
mod time;
mod user;

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct Configuration {
    homeserver_url: String,
    output_dir: String,
    total_steps: usize,
    users_per_step: usize,
    friendship_ratio: f32,
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "step_duration_in_secs")]
    step_duration: Duration,
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "tick_duration_in_secs")]
    tick_duration: Duration,
    max_users_to_act_per_tick: usize,
    waiting_period: usize,
    retry_request_config: bool,
    user_creation_retry_attempts: usize,
    user_creation_throughput: usize,
    room_creation_throughput: usize,
}

pub struct State {
    config: Configuration,
    friendships: Vec<Friendship>,
    users: Vec<User<Synching>>,
}

#[derive(serde::Serialize, Default, Debug)]
struct Report {
    homeserver: String,
    step_users: usize,
    step_friendships: usize,
    report: MetricsReport,
}

impl State {
    pub fn new(config: Configuration) -> Self {
        Self {
            config,
            friendships: vec![],
            users: vec![],
        }
    }

    async fn init_users(&mut self, tx: Sender<Event>) {
        let timestamp = time_now();
        let actual_users = self.users.len();
        let desired_users = actual_users + self.config.users_per_step;
        let server = self.config.homeserver_url.clone();
        let retry_enabled = self.config.retry_request_config;
        let retry_attempts = self.config.user_creation_retry_attempts;

        let progress_bar = create_progress_bar(
            "Init users".to_string(),
            (desired_users - actual_users).try_into().unwrap(),
        );
        progress_bar.tick();

        let mut user_creations_buffer = iter((actual_users..desired_users).map(|i| {
            create_user(
                server.clone(),
                &progress_bar,
                tx.clone(),
                i,
                retry_attempts,
                timestamp,
                retry_enabled,
            )
        }))
        .buffer_unordered(self.config.user_creation_throughput);

        while let Some(user) = user_creations_buffer.next().await {
            if let Some(user) = user {
                self.users.push(user);
            }
        }

        progress_bar.finish_and_clear();
    }

    fn calculate_step_friendships(&self) -> usize {
        let total_users = self.users.len();
        let max_friendships = (total_users * (total_users - 1)) / 2;
        ((max_friendships as f32) * self.config.friendship_ratio).ceil() as usize
    }

    async fn init_friendships(&mut self) {
        let amount_of_friendships = self.calculate_step_friendships();

        let progress_bar = create_progress_bar(
            "Init friendhips".to_string(),
            (amount_of_friendships - self.friendships.len())
                .try_into()
                .unwrap(),
        );
        progress_bar.tick();

        let mut futures = vec![];

        while self.friendships.len() < amount_of_friendships {
            let (first_user, second_user) = self.get_random_friendship();

            futures.push(join_users_to_room(first_user, second_user, &progress_bar));
        }

        let stream_iter = iter(futures);
        let mut buffered_iter = stream_iter.buffer_unordered(self.config.room_creation_throughput);

        while (buffered_iter.next().await).is_some() {}

        self.friendships.sort();

        progress_bar.finish_and_clear();
    }

    fn get_random_friendship(&mut self) -> (&User<Synching>, &User<Synching>) {
        loop {
            let mut rng = rand::thread_rng();
            let first_user = self.users.iter().choose(&mut rng).unwrap();
            let second_user = self.users.iter().choose(&mut rng).unwrap();
            if first_user.id() == second_user.id() {
                continue;
            }
            let friendship = Friendship::from_users(first_user, second_user);
            if self.friendships.contains(&friendship) {
                continue;
            }
            self.friendships.push(friendship);

            break (first_user, second_user);
        }
    }

    async fn act(&mut self, tx: Sender<Event>) {
        let start = Instant::now();

        let users_to_act = std::cmp::min(self.users.len(), self.config.max_users_to_act_per_tick);
        let progress_bar = create_progress_bar(
            "Running",
            (self.config.step_duration.as_secs_f64() / self.config.tick_duration.as_secs_f64())
                .ceil() as u64,
        );

        progress_bar.tick();
        let mut rng = rand::thread_rng();
        loop {
            if start.elapsed().ge(&self.config.step_duration) {
                // elapsed time for current step reached, breaking the loop and proceed to next step
                break;
            }
            let loop_start = Instant::now();

            let mut controller = TaskController::with_timeout(self.config.tick_duration);

            let mut handles = vec![];
            for user in self.users.iter().choose_multiple(&mut rng, users_to_act) {
                // Every spawn result in a tokio::select! with the future and the timeout
                handles.push(controller.spawn({
                    let mut user = user.clone();
                    async move {
                        user.act().await;
                    }
                }));
            }

            // Timeout is contemplated in this join_all because of the controller spawning tasks.
            join_all(handles).await;

            // If elapsed time of the current iteration is less than tick duration, we wait until that time.
            let elapsed = loop_start.elapsed();
            if elapsed.le(&self.config.tick_duration) {
                sleep(self.config.tick_duration - elapsed);
            }
            progress_bar.inc(1);
        }
        progress_bar.finish_and_clear();

        tx.send(Event::AllMessagesSent)
            .await
            .expect("AllMessagesSent event");
    }

    async fn waiting_period(&self, tx: Sender<Event>, metrics: &Metrics) {
        let spinner = ProgressBar::new_spinner()
            .with_style(
                ProgressStyle::default_spinner()
                    .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
                    .template("{prefix:.bold.dim} {spinner} {wide_msg}"),
            )
            .with_prefix("Tear down:");

        let waiting_time = Duration::from_secs(self.config.waiting_period as u64);
        let one_sec = Duration::from_secs(1);
        let start = Instant::now();
        while !metrics.all_messages_received().await {
            if start.elapsed().ge(&waiting_time) {
                break;
            }
            let wait_one_sec = Instant::now();
            spinner.set_message("Waiting for messages...");
            loop {
                if wait_one_sec.elapsed().ge(&one_sec) {
                    break;
                }
                sleep(Duration::from_millis(100));
                spinner.inc(1);
            }

            spinner.set_message("Checking all messages were received...");
        }

        tx.send(Event::Finish).await.expect("Finish event sent");
    }

    pub async fn run(&mut self) {
        println!("{:#?}\n", self.config);

        let execution_id = time_now();

        let (tx, rx) = mpsc::channel::<Event>(100);
        let metrics = Metrics::new(rx);
        for step in 1..=self.config.total_steps {
            println!("Running step {}", step);

            let handle = metrics.run();

            // step warm up
            self.init_users(tx.clone()).await;
            self.init_friendships().await;

            // step running
            self.act(tx.clone()).await;
            self.waiting_period(tx.clone(), &metrics).await;

            // generate report
            let report = handle.await.expect("read events loop should end correctly");
            self.generate_report(execution_id, step, report);

            // print new line in between steps
            if step < self.config.total_steps {
                println!();
            }
        }
    }

    pub fn generate_report(&self, execution_id: u128, step: usize, report: MetricsReport) {
        let result = create_dir_all(format!("{}/{}", self.config.output_dir, execution_id));
        let output_dir = if result.is_err() {
            println!(
                "Couldn't ensure output folder, defaulting to 'output/{}'",
                execution_id
            );
            create_dir_all(format!("output/{}", execution_id)).unwrap();
            "output"
        } else {
            self.config.output_dir.as_ref()
        };

        let path = format!(
            "{}/{}/report_{}_{}.yaml",
            output_dir,
            execution_id,
            step,
            time_now()
        );
        let buffer = File::create(&path).unwrap();

        let report = Report {
            homeserver: self.config.homeserver_url.to_string(),
            step_users: self.users.len(),
            step_friendships: self.friendships.len(),
            report,
        };

        serde_yaml::to_writer(buffer, &report).expect("Couldn't write report to file");
        println!("Step report generated: {}\n", path);
        println!("{:#?}\n", report);
    }
}
