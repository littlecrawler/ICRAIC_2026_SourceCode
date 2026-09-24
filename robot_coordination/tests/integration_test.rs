use robot_coordination::*;
use std::thread;
use std::time::Duration;

#[test]
fn test_robot_registration() {
    let coordinator = RobotCoordinator::new(5);

    let robot1 = Robot::new(1, "Robot-1");
    let robot2 = Robot::new(2, "Robot-2");

    assert!(coordinator.register_robot(robot1).is_ok());
    assert!(coordinator.register_robot(robot2).is_ok());
    assert_eq!(coordinator.robot_count(), 2);

    // failed to register duplicate robot ID
    let robot_dup = Robot::new(1, "Robot-1-Dup");
    assert!(coordinator.register_robot(robot_dup).is_err());
}

#[test]
fn test_task_queue() {
    let coordinator = RobotCoordinator::new(3);

    // add tasks
    let task1 = Task::new(101, 1, 100, "Clean room 101");
    let task2 = Task::new(102, 2, 150, "Deliver medicine");

    coordinator.add_task(task1);
    coordinator.add_task(task2);

    assert_eq!(coordinator.queue_length(), 2);

    // register a robot and request a task
    let robot = Robot::new(1, "TestBot");
    coordinator.register_robot(robot).unwrap();

    // request a task
    let task = coordinator.request_task(1);
    assert!(task.is_some());
    assert_eq!(coordinator.queue_length(), 1);

    // complete the task
    coordinator.complete_task(1).unwrap();
}

#[test]
fn test_zone_mutual_exclusion() {
    let coordinator = RobotCoordinator::new(2);

    let robot1 = Robot::new(1, "Robot-1");
    let robot2 = Robot::new(2, "Robot-2");

    coordinator.register_robot(robot1).unwrap();
    coordinator.register_robot(robot2).unwrap();

    // robot 1 enters zone 0 - acquires the lock
    let guard1 = coordinator.request_zone(1, 0).unwrap();

    // robot 2 cannot enter the same zone - failed to acquire lock
    assert!(coordinator.request_zone(2, 0).is_err());

    // robot 2 can enter a different zone
    let guard2 = coordinator.request_zone(2, 1).unwrap();

    // leave zones
    coordinator.leave_zone(guard1, 1, 0);
    coordinator.leave_zone(guard2, 2, 1);
}
#[test]
fn test_heartbeat_timeout() {
    let coordinator = RobotCoordinator::new(3);

    let robot = Robot::new(1, "TestBot");
    coordinator.register_robot(robot).unwrap();

    // initially online
    assert_eq!(coordinator.get_online_robots().len(), 1);

    // simulate heartbeat timeout by sleeping longer than the timeout period
    thread::sleep(Duration::from_secs(4));

    // check heartbeats and get timed out robots
    let timed_out = coordinator.check_heartbeats();
    assert_eq!(timed_out.len(), 1);
    assert_eq!(timed_out[0], 1);

    // robot should now be marked as offline
    assert_eq!(coordinator.get_online_robots().len(), 0);
    assert_eq!(coordinator.get_offline_robots().len(), 1);

    // simulate robot coming back online by sending a heartbeat
    coordinator.heartbeat(1);
    assert_eq!(coordinator.get_online_robots().len(), 1);
}

#[test]
fn test_busy_zone_requeues_task_until_it_can_run() {
    let coordinator = RobotCoordinator::new(1);
    coordinator
        .register_robot(Robot::new(1, "Blocker"))
        .unwrap();
    coordinator.register_robot(Robot::new(2, "Worker")).unwrap();
    coordinator.add_task(Task::new(201, 0, 10, "Retry-safe task"));

    let blocking_guard = coordinator.request_zone(1, 0).unwrap();
    match coordinator.dispatch_next_task(2).unwrap() {
        DispatchOutcome::ZoneBusy { task_id, zone } => {
            assert_eq!(task_id, 201);
            assert_eq!(zone, 0);
        }
        _ => panic!("the occupied zone should deny the first dispatch"),
    }
    assert_eq!(
        coordinator.queue_length(),
        1,
        "denied task must remain queued"
    );

    assert!(coordinator.leave_zone(blocking_guard, 1, 0));
    match coordinator.dispatch_next_task(2).unwrap() {
        DispatchOutcome::Assigned { task, zone_guard } => {
            assert_eq!(task.id, 201);
            assert!(coordinator.leave_zone(zone_guard, 2, task.zone as usize));
            coordinator.complete_task(2).unwrap();
        }
        _ => panic!("the requeued task should run after the zone is released"),
    }

    let stats = coordinator.get_stats();
    assert_eq!(coordinator.queue_length(), 0);
    assert_eq!(stats.total_tasks_created, 1);
    assert_eq!(stats.total_tasks_completed, 1);
    assert_eq!(stats.total_zone_denials, 1);
}

#[test]
fn test_concurrent_operations() {
    use std::sync::Arc;

    let coordinator = Arc::new(RobotCoordinator::new(5));

    // register multiple robots
    for i in 0..3 {
        let robot = Robot::new(i, &format!("Robot-{}", i));
        coordinator.register_robot(robot).unwrap();
    }

    // add multiple tasks
    for i in 0..10 {
        let task = Task::new(100 + i, i % 3, 50, &format!("Task {}", i));
        coordinator.add_task(task);
    }

    // spawn threads for each robot to perform operations concurrently
    let mut handles = vec![];

    for robot_id in 0..3 {
        let coord = Arc::clone(&coordinator);
        let handle = thread::spawn(move || {
            loop {
                coord.heartbeat(robot_id);

                match coord.dispatch_next_task(robot_id).unwrap() {
                    DispatchOutcome::Assigned { task, zone_guard } => {
                        thread::sleep(Duration::from_millis(50));
                        assert!(coord.leave_zone(zone_guard, robot_id, task.zone as usize));
                        coord.complete_task(robot_id).unwrap();
                    }
                    DispatchOutcome::ZoneBusy { .. } => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    DispatchOutcome::QueueEmpty => {
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }

    // wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // check final statistics
    let stats = coordinator.get_stats();
    println!("{:?}", stats);
    assert_eq!(stats.total_tasks_created, 10);
    assert_eq!(stats.total_tasks_completed, 10);
    assert_eq!(stats.total_zone_access_grants, 10);
    assert_eq!(coordinator.queue_length(), 0);
}
