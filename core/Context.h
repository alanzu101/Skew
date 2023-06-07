//
// Created by Yi Lu on 7/19/18.
// Modified by Alan Zu on 3/2/2023.
//

#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace aria {
class Context {

public:
  void set_star_partitioner() {
    if (protocol != "Star") {
      return;
    }
    if (coordinator_id == 0) {
      partitioner = "StarS";
    } else {
      partitioner = "StarC";
    }
  }

public:
  std::size_t coordinator_id = 0;
  std::size_t partition_num = 2;
  std::size_t worker_num = 8;
  std::size_t coordinator_num = 0;
  std::size_t io_thread_num = 1;
  std::string protocol;
  std::string replica_group = "1";   //"1,3" for Skew, calvin
  std::string lock_manager;
  std::string scheduler = "2";   //"3,6" for Skew
  std::size_t batch_size = 100; //240 star, calvin, Skew, dbx batch size
  std::size_t batch_flush = 10;
  int time_to_run = 25;   // seconds to run for Coordinator
  int time_to_warmup = 5; 
  int time_to_cooldown = 5; 
  std::size_t group_time = 40; // ms
  std::size_t sleep_time = 50; // us
  std::string partitioner;
  std::size_t delay_time = 0;
  std::string log_path = "./temp/";
  std::string cdf_path;
  std::size_t cpu_core_id = 0;

  std::size_t durable_write_cost = 0;

  bool tcp_no_delay = true;
  bool tcp_quick_ack = false;

  bool cpu_affinity = true;

  bool sleep_on_retry = true;

  bool exact_group_commit = false;

  bool mvcc = false;
  bool bohm_local = false;
  bool bohm_single_spin = false;

  bool read_on_replica = false;
  bool local_validation = false;
  bool rts_sync = false;
  bool star_sync_in_single_master_phase = false;
  bool star_dynamic_batch_size = true;
  bool parallel_locking_and_validation = true;

  bool same_batch = false; // skew, calvin and bohm

  bool aria_read_only_optmization = true;
  bool aria_reordering_optmization = true;
  bool aria_snapshot_isolation = false;

  std::size_t ariaFB_lock_manager;

  bool pwv_ycsb_star = false;
  bool is_ycsb = false;     // for skew

  bool operation_replication = false;

  std::vector<std::string> peers;
};
} // namespace aria