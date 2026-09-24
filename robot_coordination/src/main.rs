use robot_coordination::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Demo 1: Multiple robots concurrently requesting tasks
pub fn demo_concurrent_tasks() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  DEMO 1: Multiple Robots Concurrently Requesting Tasks  ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let coordinator = Arc::new(RobotCoordinator::new(3));

    // Register 3 robots
    for i in 1..=3 {
        let robot = Robot::new(i, &format!("WorkerBot-{}", i));
        coordinator.register_robot(robot).unwrap();
        println!("✅ Registered: WorkerBot-{}", i);
    }

    // Add 9 tasks across 3 zones
    let tasks = vec![
        Task::new(101, 0, 300, "Clean Room A1"),
        Task::new(102, 1, 400, "Deliver Meds to B1"),
        Task::new(103, 2, 350, "Assist Surgery C1"),
        Task::new(104, 0, 250, "Clean Room A2"),
        Task::new(105, 1, 450, "Deliver Meds to B2"),
        Task::new(106, 2, 300, "Assist Surgery C2"),
        Task::new(107, 0, 200, "Clean Room A3"),
        Task::new(108, 1, 500, "Deliver Meds to B3"),
        Task::new(109, 2, 400, "Assist Surgery C3"),
    ];

    coordinator.add_tasks(tasks);
    println!("📦 Added {} tasks to queue\n", coordinator.queue_length());

    // Start monitoring
    coordinator.start_monitoring();

    // Create 3 robot threads working concurrently
    let mut handles = vec![];

    for robot_id in 1..=3 {
        let coord = Arc::clone(&coordinator);
        let handle = thread::spawn(move || {
            let robot_name = format!("WorkerBot-{}", robot_id);
            let mut attempt = 0;

            loop {
                attempt += 1;
                // Send heartbeat
                coord.heartbeat(robot_id);

                match coord.dispatch_next_task(robot_id).unwrap() {
                    DispatchOutcome::Assigned { task, zone_guard } => {
                        println!(
                            "🤖 [{}] Attempt {}: Got task '{}' (Zone {})",
                            robot_name, attempt, task.description, task.zone
                        );
                        println!("   ✅ [{}] Entered Zone {}", robot_name, task.zone);

                        thread::sleep(Duration::from_millis(task.duration_ms));

                        println!(
                            "   ✅ [{}] Completed work in Zone {}",
                            robot_name, task.zone
                        );
                        coord.leave_zone(zone_guard, robot_id, task.zone as usize);
                        coord.complete_task(robot_id).unwrap();
                    }
                    DispatchOutcome::ZoneBusy { task_id, zone } => {
                        println!(
                            "   ⏳ [{}] Task {} requeued because Zone {} is busy",
                            robot_name, task_id, zone
                        );
                        thread::sleep(Duration::from_millis(10));
                    }
                    DispatchOutcome::QueueEmpty => {
                        break;
                    }
                }
            }

            println!("🏁 [{}] Finished all available work!", robot_name);
        });
        handles.push(handle);
    }

    // Wait for all robots to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Stop monitoring
    coordinator.stop_monitoring();

    // Print stats
    let stats = coordinator.get_stats();
    println!("\n📊 DEMO 1 Statistics:");
    println!("   Tasks Created: {}", stats.total_tasks_created);
    println!("   Tasks Completed: {}", stats.total_tasks_completed);
    println!("   Tasks Remaining: {}", coordinator.queue_length());
    println!(
        "   Zone Access Attempts: {}",
        stats.total_zone_access_attempts
    );
    println!("   Zone Access Grants: {}", stats.total_zone_access_grants);
    println!("   Zone Denials: {}", stats.total_zone_denials);

    assert_eq!(
        stats.total_tasks_completed, 9,
        "All tasks should be completed!"
    );
    println!("\n✅ DEMO 1 PASSED: Multiple robots worked concurrently!\n");
}

/// Demo 2: Safe access to shared zones (mutual exclusion)
pub fn demo_zone_mutual_exclusion() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  DEMO 2: Safe Access to Shared Zones (Mutual Exclusion) ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let coordinator = Arc::new(RobotCoordinator::new(1)); // Only 1 zone for testing

    // Register 2 robots
    coordinator.register_robot(Robot::new(1, "Bot-A")).unwrap();
    coordinator.register_robot(Robot::new(2, "Bot-B")).unwrap();
    println!("✅ Registered: Bot-A and Bot-B");
    println!("🎯 Testing with a single zone (Zone 0)\n");

    coordinator.start_monitoring();

    // Add a long task that will occupy the zone
    coordinator.add_task(Task::new(201, 0, 1500, "Long Critical Operation"));

    let coord1 = Arc::clone(&coordinator);
    let handle1 = thread::spawn(move || {
        coord1.heartbeat(1);
        println!("🔵 [Bot-A] Attempting to get a task...");

        match coord1.dispatch_next_task(1).unwrap() {
            DispatchOutcome::Assigned { task, zone_guard } => {
                println!("🔵 [Bot-A] Got task: '{}'", task.description);
                println!("🔵 [Bot-A] ✅ Acquired Zone {}!", task.zone);
                println!("🔵 [Bot-A] Performing critical operation (1.5 sec)...");
                thread::sleep(Duration::from_millis(task.duration_ms));
                println!("🔵 [Bot-A] ✅ Operation complete!");
                coord1.leave_zone(zone_guard, 1, task.zone as usize);
                coord1.complete_task(1).unwrap();
                println!("🔵 [Bot-A] Released Zone {}", task.zone);
            }
            _ => panic!("Bot-A should receive the first task"),
        }
    });

    // Give Bot-A time to acquire the zone
    thread::sleep(Duration::from_millis(500));

    let coord2 = Arc::clone(&coordinator);
    let handle2 = thread::spawn(move || {
        coord2.heartbeat(2);
        println!("🟢 [Bot-B] Attempting to get a task...");

        // Add another task for Bot-B
        coord2.add_task(Task::new(202, 0, 500, "Quick Task"));
        thread::sleep(Duration::from_millis(100));

        match coord2.dispatch_next_task(2).unwrap() {
            DispatchOutcome::ZoneBusy { task_id, zone } => {
                println!(
                    "🟢 [Bot-B] Task {} could not enter occupied Zone {}",
                    task_id, zone
                );
                println!("   ✅ Mutual exclusion WORKING; task safely requeued!");
            }
            _ => panic!("Bot-B's first dispatch should be denied while Zone 0 is occupied"),
        }

        thread::sleep(Duration::from_millis(1000));
        coord2.heartbeat(2);
        match coord2.dispatch_next_task(2).unwrap() {
            DispatchOutcome::Assigned { task, zone_guard } => {
                println!("🟢 [Bot-B] Retried task '{}'", task.description);
                thread::sleep(Duration::from_millis(task.duration_ms));
                coord2.leave_zone(zone_guard, 2, task.zone as usize);
                coord2.complete_task(2).unwrap();
            }
            _ => panic!("Bot-B's requeued task should succeed after Bot-A leaves"),
        }
    });

    handle1.join().unwrap();
    handle2.join().unwrap();

    coordinator.stop_monitoring();

    let stats = coordinator.get_stats();
    println!("\n📊 DEMO 2 Statistics:");
    println!(
        "   Zone Access Attempts: {}",
        stats.total_zone_access_attempts
    );
    println!("   Zone Access Grants: {}", stats.total_zone_access_grants);
    println!("   Zone Denials: {}", stats.total_zone_denials);

    assert_eq!(stats.total_zone_denials, 1, "Bot-B should be denied once");
    assert_eq!(stats.total_tasks_completed, 2, "No task should be lost");
    assert_eq!(
        coordinator.queue_length(),
        0,
        "The requeued task should finish"
    );
    println!("\n✅ DEMO 2 PASSED: Mutual exclusion and safe retry work correctly!\n");
}

/// Demo 3: Robot timing out and being marked offline
pub fn demo_timeout() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  DEMO 3: Robot Timeout Detection & Recovery              ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let coordinator = Arc::new(RobotCoordinator::new(2));

    // Register 3 robots
    coordinator
        .register_robot(Robot::new(1, "HeartbeatBot"))
        .unwrap();
    coordinator
        .register_robot(Robot::new(2, "TimeoutBot"))
        .unwrap();
    coordinator
        .register_robot(Robot::new(3, "RecoveryBot"))
        .unwrap();
    println!("✅ Registered: HeartbeatBot, TimeoutBot, RecoveryBot\n");

    // Send initial heartbeats
    coordinator.heartbeat(1);
    coordinator.heartbeat(2);
    coordinator.heartbeat(3);

    coordinator.start_monitoring();

    // Helper function to print status
    fn print_status(coord: &RobotCoordinator, msg: &str) {
        let online: Vec<_> = coord.get_online_robots().iter().map(|r| r.id).collect();
        let offline: Vec<_> = coord.get_offline_robots().iter().map(|r| r.id).collect();
        println!("{}", msg);
        println!("   Online robots: {:?}", online);
        println!("   Offline robots: {:?}", offline);
    }

    print_status(&coordinator, "📊 Initial status:");

    // Phase 1: HeartbeatBot keeps sending heartbeats, TimeoutBot also sends
    println!("\n⏰ Phase 1: All robots sending heartbeats...");
    for i in 1..=3 {
        thread::sleep(Duration::from_secs(1));
        coordinator.heartbeat(1);
        coordinator.heartbeat(2);
        coordinator.heartbeat(3);
        println!("   💓 Heartbeat #{} from all robots", i);
    }

    print_status(&coordinator, "\n📊 After 3 seconds (all active):");

    // Phase 2: TimeoutBot stops sending heartbeats (others continue)
    println!("\n⏰ Phase 2: TimeoutBot (ID 2) stops sending heartbeats...");
    println!("   HeartbeatBot and RecoveryBot continue...");

    for i in 1..=3 {
        thread::sleep(Duration::from_secs(1));
        // Only robot 1 and 3 send heartbeats
        coordinator.heartbeat(1);
        coordinator.heartbeat(3);
        println!("   💓 Heartbeat #{} from HeartbeatBot and RecoveryBot", i);
    }

    // Check for timeouts
    let timed_out = coordinator.check_heartbeats();
    println!("\n   ⏰ Timeout detected for robots: {:?}", timed_out);

    print_status(&coordinator, "\n📊 After TimeoutBot stops (3 seconds):");

    // Phase 3: TimeoutBot reconnects
    println!("\n⏰ Phase 3: TimeoutBot sends heartbeat again (recovery)...");
    coordinator.heartbeat(2);
    thread::sleep(Duration::from_millis(500));
    coordinator.check_heartbeats();

    print_status(&coordinator, "\n📊 After recovery:");

    coordinator.stop_monitoring();

    let stats = coordinator.get_stats();
    println!("\n📊 DEMO 3 Statistics:");
    println!(
        "   Heartbeats Received: {}",
        stats.total_heartbeats_received
    );
    println!("   Timeouts Detected: {}", stats.total_timeouts_detected);

    // Should have detected exactly 1 timeout (only TimeoutBot)
    assert_eq!(
        stats.total_timeouts_detected, 1,
        "Should have detected exactly 1 timeout! Found {} timeouts.",
        stats.total_timeouts_detected
    );
    println!("\n✅ DEMO 3 PASSED: Timeout detection and recovery work correctly!\n");
}

/// Run all demos
pub fn run_all_demos() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║     TARDEM HEALTHCARE ROBOT COORDINATION - FULL DEMO      ║");
    println!("║     Demonstrating Concurrency, Synchronization, & Safety  ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    // Demo 1: Concurrent task processing
    demo_concurrent_tasks();

    // Demo 2: Mutual exclusion
    demo_zone_mutual_exclusion();

    // Demo 3: Timeout detection
    demo_timeout();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  🎉 ALL DEMOS PASSED!                                      ║");
    println!("║  ✅ Multiple robots working concurrently                   ║");
    println!("║  ✅ Safe zone access with mutual exclusion                 ║");
    println!("║  ✅ Robot timeout detection and recovery                   ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}

fn main() {
    run_all_demos();
}
