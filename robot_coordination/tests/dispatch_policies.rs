use robot_coordination::{DispatchOutcome, DispatchPolicy, Robot, RobotCoordinator, Task};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const POLICIES: [DispatchPolicy; 3] = [
    DispatchPolicy::FifoBlocking,
    DispatchPolicy::RetryTail,
    DispatchPolicy::ReadyScan,
];

#[test]
fn ready_scan_bypasses_busy_zone_without_reordering_skipped_tasks() {
    let c = RobotCoordinator::new(2);
    for id in 0..3 {
        c.register_robot(Robot::new(id, "robot")).unwrap();
    }
    c.add_tasks(vec![
        Task::new(10, 0, 1, "first"),
        Task::new(11, 0, 1, "second"),
        Task::new(12, 1, 1, "ready"),
    ]);
    let held = c.request_zone(0, 0).unwrap();
    let before = c.get_stats();
    match c
        .dispatch_next_task_with_policy(1, DispatchPolicy::ReadyScan)
        .unwrap()
    {
        DispatchOutcome::Assigned { task, zone_guard } => {
            assert_eq!(task.id, 12);
            assert!(c.leave_zone(zone_guard, 1, 1));
            c.complete_task(1).unwrap();
        }
        _ => panic!("available-zone task must be selected"),
    }
    assert_eq!(
        c.get_stats().total_zone_access_attempts - before.total_zone_access_attempts,
        2
    );
    assert!(c.leave_zone(held, 0, 0));
    for expected in [10, 11] {
        match c
            .dispatch_next_task_with_policy(2, DispatchPolicy::ReadyScan)
            .unwrap()
        {
            DispatchOutcome::Assigned { task, zone_guard } => {
                assert_eq!(task.id, expected);
                assert!(c.leave_zone(zone_guard, 2, 0));
                c.complete_task(2).unwrap();
            }
            _ => panic!("skipped tasks must stay queued"),
        }
    }
}

#[test]
fn all_policies_complete_each_task_id_once_under_contention() {
    for policy in POLICIES {
        let c = Arc::new(RobotCoordinator::new(3));
        for id in 0..6 {
            c.register_robot(Robot::new(id, "robot")).unwrap();
        }
        c.add_tasks((0..60).map(|id| Task::new(id, id % 3, 1, "task")).collect());
        let handles: Vec<_> = (0..6)
            .map(|worker| {
                let c = Arc::clone(&c);
                thread::spawn(move || {
                    let mut ids = Vec::new();
                    loop {
                        c.heartbeat(worker);
                        match c.dispatch_next_task_with_policy(worker, policy).unwrap() {
                            DispatchOutcome::Assigned { task, zone_guard } => {
                                thread::sleep(Duration::from_millis(task.duration_ms));
                                ids.push(task.id);
                                assert!(c.leave_zone(zone_guard, worker, task.zone as usize));
                                c.complete_task(worker).unwrap();
                            }
                            DispatchOutcome::ZoneBusy { .. } => thread::yield_now(),
                            DispatchOutcome::QueueEmpty => break,
                        }
                    }
                    ids
                })
            })
            .collect();
        let mut ids: Vec<u32> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..60).collect::<Vec<_>>());
        assert_eq!(c.get_stats().total_tasks_completed, 60);
        assert_eq!(c.queue_length(), 0);
    }
}

#[test]
fn offline_rejection_preserves_task_and_heartbeat_restores_idle_dispatch() {
    for policy in POLICIES {
        let c = RobotCoordinator::new(1);
        let mut robot = Robot::new(0, "silent");
        robot.last_heartbeat = Instant::now() - Duration::from_secs(4);
        c.register_robot(robot).unwrap();
        c.add_task(Task::new(7, 0, 1, "delivery"));
        assert!(c.dispatch_next_task_with_policy(0, policy).is_err());
        assert_eq!(c.queue_length(), 1);
        assert_eq!(c.check_heartbeats(), vec![0]);
        c.heartbeat(0);
        match c.dispatch_next_task_with_policy(0, policy).unwrap() {
            DispatchOutcome::Assigned { task, zone_guard } => {
                assert_eq!(task.id, 7);
                assert!(c.leave_zone(zone_guard, 0, 0));
                c.complete_task(0).unwrap();
            }
            _ => panic!("restored idle worker should accept queued work"),
        }
    }
}

#[test]
fn invalid_zone_preserves_queue_for_every_policy() {
    for policy in POLICIES {
        let c = RobotCoordinator::new(1);
        c.register_robot(Robot::new(0, "robot")).unwrap();
        c.add_task(Task::new(7, 3, 1, "invalid"));
        assert!(c.dispatch_next_task_with_policy(0, policy).is_err());
        assert_eq!(c.queue_length(), 1);
        assert_eq!(c.idle_robot_count(), 1);
    }
}

#[test]
fn controlled_drop_ablation_exposes_loss_in_separate_dequeue_and_lock() {
    // This intentionally incorrect caller is a failure witness, never a speed baseline.
    let c = RobotCoordinator::new(1);
    for id in 0..2 {
        c.register_robot(Robot::new(id, "robot")).unwrap();
    }
    c.add_task(Task::new(7, 0, 1, "delivery"));
    let held = c.request_zone(0, 0).unwrap();
    let abandoned = c.request_task(1).unwrap();
    assert!(c.request_zone(1, abandoned.zone as usize).is_err());
    drop(abandoned);
    assert_eq!(c.queue_length(), 0);
    assert_eq!(c.get_stats().total_tasks_completed, 0);
    assert!(c.leave_zone(held, 0, 0));
    // Safe policies retain the same denied task; tested separately with exact IDs.
}

#[test]
fn ready_scan_denial_keeps_all_tasks_when_every_zone_is_occupied() {
    let c = RobotCoordinator::new(1);
    for id in 0..2 {
        c.register_robot(Robot::new(id, "robot")).unwrap();
    }
    c.add_tasks((0..12).map(|id| Task::new(id, 0, 1, "task")).collect());
    let held = c.request_zone(0, 0).unwrap();
    let before = c.get_stats().total_zone_access_attempts;
    assert!(matches!(
        c.dispatch_next_task_with_policy(1, DispatchPolicy::ReadyScan)
            .unwrap(),
        DispatchOutcome::ZoneBusy { .. }
    ));
    assert_eq!(c.queue_length(), 12);
    assert_eq!(c.get_stats().total_zone_access_attempts - before, 1);
    assert!(c.leave_zone(held, 0, 0));
}
