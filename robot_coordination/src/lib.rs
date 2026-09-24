use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::thread;
use std::time::{Duration, Instant};

/// Task Structure
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub id: u32,
    pub zone: u32,        // Task Zone
    pub duration_ms: u64, // Task Duration in milliseconds
    pub description: String,
}

impl Task {
    pub fn new(id: u32, zone: u32, duration_ms: u64, description: &str) -> Self {
        Task {
            id,
            zone,
            duration_ms,
            description: description.to_string(),
        }
    }
}

/// Robot Status Enum
#[derive(Debug, Clone, PartialEq)]
pub enum RobotStatus {
    Online,
    Offline, // heartbeat timeout
    Busy,
    Idle,
}

/// Robot Structure
#[derive(Debug, Clone)]
pub struct Robot {
    pub id: u32,
    pub name: String,
    pub status: RobotStatus,
    pub current_zone: Option<u32>,
    pub last_heartbeat: Instant,
    pub tasks_completed: u32,
}

impl Robot {
    pub fn new(id: u32, name: &str) -> Self {
        Robot {
            id,
            name: name.to_string(),
            status: RobotStatus::Idle,
            current_zone: None,
            last_heartbeat: Instant::now(),
            tasks_completed: 0,
        }
    }

    /// Update heartbeat timestamp and status
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        if self.status == RobotStatus::Offline {
            self.status = RobotStatus::Idle; // Online again
        }
    }

    /// Timeout check for robot status
    pub fn is_timed_out(&self) -> bool {
        self.last_heartbeat.elapsed() > Duration::from_secs(3)
    }
}

/// Robot Coordinator Structure
pub struct RobotCoordinator {
    // Task Queue
    task_queue: Arc<Mutex<VecDeque<Task>>>,

    // Zone Locks: One Mutex per zone for exclusive access
    zone_locks: Arc<Vec<Mutex<()>>>,

    // Robot List: Read-write lock allowing multiple readers or a single writer
    robots: Arc<RwLock<Vec<Robot>>>,

    // stats
    stats: Arc<Mutex<CoordinatorStats>>,

    // monitor
    monitoring_active: Arc<Mutex<bool>>,
}

/// Coordinator Statistics Structure
#[derive(Debug, Default, Clone)]
pub struct CoordinatorStats {
    pub total_tasks_created: u32,
    pub total_tasks_completed: u32,
    pub total_zone_access_attempts: u32,
    pub total_zone_access_grants: u32,
    pub total_zone_denials: u32, // denials due to zone locks
    pub total_heartbeats_received: u32,
    pub total_timeouts_detected: u32,
}

/// Outcome of an atomic task dispatch attempt.
///
/// A task is removed from the queue only when its required zone lock is
/// acquired. If the zone is busy, the task is returned to the queue so that a
/// later attempt can execute it instead of silently losing it.
pub enum DispatchOutcome<'a> {
    Assigned {
        task: Task,
        zone_guard: MutexGuard<'a, ()>,
    },
    QueueEmpty,
    ZoneBusy {
        task_id: u32,
        zone: u32,
    },
}

impl RobotCoordinator {
    pub fn new(num_zones: u32) -> Self {
        // Initialize zone locks
        let mut zone_locks = Vec::with_capacity(num_zones as usize);
        for _ in 0..num_zones {
            zone_locks.push(Mutex::new(()));
        }

        RobotCoordinator {
            task_queue: Arc::new(Mutex::new(VecDeque::new())),
            zone_locks: Arc::new(zone_locks),
            robots: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(Mutex::new(CoordinatorStats::default())),
            monitoring_active: Arc::new(Mutex::new(false)),
        }
    }

    /// register a new robot to the coordinator
    pub fn register_robot(&self, robot: Robot) -> Result<(), String> {
        let mut robots = self.robots.write().unwrap();

        // check for duplicate robot ID
        if robots.iter().any(|r| r.id == robot.id) {
            return Err(format!("Robot with ID {} already exists", robot.id));
        }

        robots.push(robot);
        Ok(())
    }

    /// get total robot count
    pub fn robot_count(&self) -> usize {
        self.robots.read().unwrap().len()
    }

    /// get count of idle robots (not busy and not timed out)
    pub fn idle_robot_count(&self) -> usize {
        let robots = self.robots.read().unwrap();
        robots
            .iter()
            .filter(|r| r.status == RobotStatus::Idle && !r.is_timed_out())
            .count()
    }
}

impl RobotCoordinator {
    /// Add a single task to the queue
    pub fn add_task(&self, task: Task) {
        let mut queue = self.task_queue.lock().unwrap();
        queue.push_back(task);

        // Update statistics
        let mut stats = self.stats.lock().unwrap();
        stats.total_tasks_created += 1;
    }

    /// Add multiple tasks to the queue
    pub fn add_tasks(&self, tasks: Vec<Task>) {
        let tasks_len = tasks.len() as u32;
        let mut queue = self.task_queue.lock().unwrap();
        for task in tasks {
            queue.push_back(task);
        }

        // Update statistics
        let mut stats = self.stats.lock().unwrap();
        stats.total_tasks_created += tasks_len;
    }

    /// Low-level task request retained for compatibility.
    ///
    /// New coordination loops should prefer [`Self::dispatch_next_task`],
    /// which acquires the task's zone atomically and requeues the task if that
    /// zone is busy.
    pub fn request_task(&self, robot_id: u32) -> Option<Task> {
        // check robot status before assigning task
        {
            let robots = self.robots.read().unwrap();
            let robot = robots.iter().find(|r| r.id == robot_id)?;
            // if robot is offline, do not assign task
            if robot.status == RobotStatus::Offline {
                return None;
            }
        }

        // assign task from queue
        let mut queue = self.task_queue.lock().unwrap();
        let task = queue.pop_front();

        // if a task is assigned, update robot status to Busy and set current zone
        if let Some(ref t) = task {
            let mut robots = self.robots.write().unwrap();
            if let Some(robot) = robots.iter_mut().find(|r| r.id == robot_id) {
                robot.status = RobotStatus::Busy;
                robot.current_zone = Some(t.zone);
            }
        }

        task
    }

    /// Get the current length of the task queue
    pub fn queue_length(&self) -> usize {
        self.task_queue.lock().unwrap().len()
    }

    /// Atomically dispatch the next queued task and acquire its required zone.
    ///
    /// A busy zone produces [`DispatchOutcome::ZoneBusy`] and moves the task to
    /// the back of the queue. This preserves the task while allowing work for
    /// other zones to make progress on subsequent attempts.
    pub fn dispatch_next_task(&self, robot_id: u32) -> Result<DispatchOutcome<'_>, String> {
        {
            let robots = self.robots.read().unwrap();
            let robot = robots
                .iter()
                .find(|robot| robot.id == robot_id)
                .ok_or_else(|| format!("Robot {} not found", robot_id))?;

            if robot.status == RobotStatus::Offline || robot.is_timed_out() {
                return Err(format!("Robot {} is offline", robot_id));
            }
            if robot.status == RobotStatus::Busy {
                return Err(format!("Robot {} is already busy", robot_id));
            }
        }

        let mut queue = self.task_queue.lock().unwrap();
        let Some(task) = queue.pop_front() else {
            return Ok(DispatchOutcome::QueueEmpty);
        };

        let zone = task.zone as usize;
        if zone >= self.zone_locks.len() {
            queue.push_front(task);
            return Err(format!("Zone {} does not exist", zone));
        }

        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_zone_access_attempts += 1;
        }

        let zone_guard = match self.zone_locks[zone].try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                let task_id = task.id;
                let task_zone = task.zone;
                queue.push_back(task);
                drop(queue);

                let mut stats = self.stats.lock().unwrap();
                stats.total_zone_denials += 1;

                return Ok(DispatchOutcome::ZoneBusy {
                    task_id,
                    zone: task_zone,
                });
            }
        };
        drop(queue);

        {
            let mut robots = self.robots.write().unwrap();
            let robot = robots
                .iter_mut()
                .find(|robot| robot.id == robot_id)
                .ok_or_else(|| format!("Robot {} not found", robot_id))?;

            if robot.status == RobotStatus::Offline || robot.is_timed_out() {
                drop(robots);
                drop(zone_guard);
                self.task_queue.lock().unwrap().push_front(task);
                return Err(format!("Robot {} became offline", robot_id));
            }
            if robot.status == RobotStatus::Busy {
                drop(robots);
                drop(zone_guard);
                self.task_queue.lock().unwrap().push_front(task);
                return Err(format!("Robot {} became busy", robot_id));
            }

            robot.status = RobotStatus::Busy;
            robot.current_zone = Some(task.zone);
        }

        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_zone_access_grants += 1;
        }

        Ok(DispatchOutcome::Assigned { task, zone_guard })
    }

    /// Complete a task for a robot, updating its status and statistics
    pub fn complete_task(&self, robot_id: u32) -> Result<(), String> {
        let mut robots = self.robots.write().unwrap();

        if let Some(robot) = robots.iter_mut().find(|r| r.id == robot_id) {
            if robot.status == RobotStatus::Busy {
                robot.status = RobotStatus::Idle;
                robot.current_zone = None;
                robot.tasks_completed += 1;

                let mut stats = self.stats.lock().unwrap();
                stats.total_tasks_completed += 1;

                Ok(())
            } else {
                Err(format!("Robot {} is not busy", robot_id))
            }
        } else {
            Err(format!("Robot {} not found", robot_id))
        }
    }
}

impl RobotCoordinator {
    /// Request access to a zone for a robot. Returns true if access is granted, false if the zone is occupied or robot is offline.
    pub fn request_zone(&self, robot_id: u32, zone: usize) -> Result<MutexGuard<'_, ()>, String> {
        // Update statistics for zone access attempts
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_zone_access_attempts += 1;
        }

        // check if zone index is valid
        if zone >= self.zone_locks.len() {
            return Err(format!("Zone {} does not exist", zone));
        }

        // check if robot is online before attempting to access zone
        {
            let robots = self.robots.read().unwrap();
            match robots.iter().find(|r| r.id == robot_id) {
                Some(robot) => {
                    if robot.status == RobotStatus::Offline {
                        return Err(format!("Robot {} is offline", robot_id));
                    }
                }
                None => return Err(format!("Robot {} not found", robot_id)),
            }
        }

        // try to acquire the lock for the requested zone
        match self.zone_locks[zone].try_lock() {
            Ok(guard) => {
                // successfully acquired the lock, update robot's current zone
                {
                    let mut robots = self.robots.write().unwrap();
                    if let Some(robot) = robots.iter_mut().find(|r| r.id == robot_id) {
                        robot.current_zone = Some(zone as u32);
                    }
                }

                // Update statistics for zone access grants
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.total_zone_access_grants += 1;
                }

                // Note: The caller is responsible for holding onto the guard to maintain the lock, and releasing it when done (by dropping the guard)
                Ok(guard)
            }
            Err(_) => {
                // Failed to acquire lock, zone is occupied
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.total_zone_denials += 1;
                }

                Err(format!("Zone {} is occupied", zone))
            }
        }
    }

    /// Leave a zone for a robot. Returns true if successful, false if the robot was not in the zone or invalid zone index.
    pub fn leave_zone(&self, _guard: MutexGuard<'_, ()>, robot_id: u32, zone: usize) -> bool {
        // check if zone index is valid
        if zone >= self.zone_locks.len() {
            return false;
        }

        // update robot's current zone to None if it matches the zone being left
        {
            let mut robots = self.robots.write().unwrap();
            if let Some(robot) = robots.iter_mut().find(|r| r.id == robot_id) {
                // make sure the robot is actually in the zone it is trying to leave
                if robot.current_zone == Some(zone as u32) {
                    robot.current_zone = None;
                } else {
                    return false; // robot is not in the zone it is trying to leave
                }
            } else {
                return false; // robot not found
            }
        }

        // _guard will be dropped here, releasing the lock on the zone
        true
    }

    /// Check if a zone is currently occupied by any robot. Returns true if occupied, false if free or invalid zone index.
    pub fn is_zone_occupied(&self, zone: usize) -> bool {
        if zone >= self.zone_locks.len() {
            return false;
        }

        // try_lock returns Err if the lock is currently held by another thread, which indicates the zone is occupied
        self.zone_locks[zone].try_lock().is_err()
    }
}

impl RobotCoordinator {
    /// Receive a heartbeat from a robot, updating its last heartbeat timestamp and status. Returns true if the robot is found and updated, false if the robot ID is not found.
    pub fn heartbeat(&self, robot_id: u32) -> bool {
        let mut robots = self.robots.write().unwrap();

        if let Some(robot) = robots.iter_mut().find(|r| r.id == robot_id) {
            robot.heartbeat();

            let mut stats = self.stats.lock().unwrap();
            stats.total_heartbeats_received += 1;

            true
        } else {
            false
        }
    }

    /// Check for robots that have timed out (no heartbeat received within the timeout period) and update their status to Offline. Returns a list of robot IDs that were marked as Offline.
    pub fn check_heartbeats(&self) -> Vec<u32> {
        let mut timed_out_robots = Vec::new();
        let mut robots = self.robots.write().unwrap();

        for robot in robots.iter_mut() {
            if robot.is_timed_out() {
                if robot.status != RobotStatus::Offline {
                    robot.status = RobotStatus::Offline;
                    robot.current_zone = None;
                    timed_out_robots.push(robot.id);

                    let mut stats = self.stats.lock().unwrap();
                    stats.total_timeouts_detected += 1;
                }
            } else {
                // If the robot is not timed out but was previously marked as Offline, we can consider it Online again
                if robot.status == RobotStatus::Offline {
                    robot.status = RobotStatus::Idle; // or Online
                }
            }
        }

        timed_out_robots
    }

    /// get offline robot list
    pub fn get_offline_robots(&self) -> Vec<Robot> {
        let robots = self.robots.read().unwrap();
        robots
            .iter()
            .filter(|r| r.status == RobotStatus::Offline)
            .cloned()
            .collect()
    }

    /// get online robot list (not offline and not timed out)
    pub fn get_online_robots(&self) -> Vec<Robot> {
        let robots = self.robots.read().unwrap();
        robots
            .iter()
            .filter(|r| r.status != RobotStatus::Offline && !r.is_timed_out())
            .cloned()
            .collect()
    }

    /// start a background thread to monitor heartbeats and update robot statuses. Returns an Arc<Mutex<bool>> that can be used to stop the monitoring thread.
    pub fn start_monitoring(&self) -> Arc<Mutex<bool>> {
        let robots = Arc::clone(&self.robots);
        let stats = Arc::clone(&self.stats);
        let active = Arc::clone(&self.monitoring_active);

        {
            let mut active_guard = self.monitoring_active.lock().unwrap();
            *active_guard = true;
        }

        let active_clone = Arc::clone(&active);
        thread::spawn(move || {
            while *active_clone.lock().unwrap() {
                thread::sleep(Duration::from_millis(500)); // check every 500ms

                let mut timed_out = Vec::new();
                {
                    let mut robots_guard = robots.write().unwrap();

                    for robot in robots_guard.iter_mut() {
                        if robot.is_timed_out() && robot.status != RobotStatus::Offline {
                            robot.status = RobotStatus::Offline;
                            robot.current_zone = None;
                            timed_out.push(robot.id);
                        }
                    }
                }

                // Update statistics for timeouts detected
                if !timed_out.is_empty() {
                    let mut stats_guard = stats.lock().unwrap();
                    stats_guard.total_timeouts_detected += timed_out.len() as u32;
                }
            }
        });

        active
    }

    /// stop the background monitoring thread by setting the active flag to false
    pub fn stop_monitoring(&self) {
        let mut active = self.monitoring_active.lock().unwrap();
        *active = false;
    }
}

impl RobotCoordinator {
    /// get a snapshot of the current coordinator statistics
    pub fn get_stats(&self) -> CoordinatorStats {
        self.stats.lock().unwrap().clone()
    }

    /// print a status report of the coordinator, including task queue length, robot statuses, and statistics
    pub fn print_status_report(&self) {
        let stats = self.stats.lock().unwrap();
        let robots = self.robots.read().unwrap();
        let queue_len = self.task_queue.lock().unwrap().len();

        println!("===== Robot Coordinator Status Report =====");
        println!("Task Queue Length: {}", queue_len);
        println!("Total Robots: {}", robots.len());

        let online_count = robots
            .iter()
            .filter(|r| !r.is_timed_out() && r.status != RobotStatus::Offline)
            .count();
        let offline_count = robots
            .iter()
            .filter(|r| r.is_timed_out() || r.status == RobotStatus::Offline)
            .count();
        let busy_count = robots
            .iter()
            .filter(|r| r.status == RobotStatus::Busy && !r.is_timed_out())
            .count();
        let idle_count = robots
            .iter()
            .filter(|r| r.status == RobotStatus::Idle && !r.is_timed_out())
            .count();

        println!("Online Robots: {}", online_count);
        println!("Offline Robots: {}", offline_count);
        println!("Busy Robots: {}", busy_count);
        println!("Idle Robots: {}", idle_count);

        println!("\n--- Statistics ---");
        println!("Tasks Created: {}", stats.total_tasks_created);
        println!("Tasks Completed: {}", stats.total_tasks_completed);
        println!("Zone Access Attempts: {}", stats.total_zone_access_attempts);
        println!("Zone Access Grants: {}", stats.total_zone_access_grants);
        println!("Zone Denials: {}", stats.total_zone_denials);
        println!("Heartbeats Received: {}", stats.total_heartbeats_received);
        println!("Timeouts Detected: {}", stats.total_timeouts_detected);

        if stats.total_tasks_created > 0 {
            let completion_rate =
                (stats.total_tasks_completed as f64 / stats.total_tasks_created as f64) * 100.0;
            println!("Task Completion Rate: {:.1}%", completion_rate);
        }

        println!("=================================");
    }
}
