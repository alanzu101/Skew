//
// Created by Yi Lu
// Modified by Alan Zu
//

#pragma once

#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>
#include <sstream>

#include <glog/logging.h>
#include "protocol/Skew/SkewScheduleMap.h" 
#include "protocol/Skew/SkewRWKey.h" 

namespace aria {

class SkewHelper {

public:
  using MetaDataType = std::atomic<uint64_t>;

  static std::vector<std::size_t> string_to_vint(const std::string &str) {
    std::vector<std::string> vstr;
    boost::algorithm::split(vstr, str, boost::is_any_of(","));
    std::vector<std::size_t> vint;
    for (auto i = 0u; i < vstr.size(); i++) {
      vint.push_back(std::atoi(vstr[i].c_str()));
    }
    return vint;
  }

  static std::size_t
  n_scheduler(std::size_t replica_group_id, std::size_t id,
                 const std::vector<std::size_t> &schedulers) {
    CHECK(replica_group_id < schedulers.size());
    return schedulers[replica_group_id];
  }

  // assume there are n = 2 scheduler and m = 4 workers
  // the following function maps
  // (2, 2, 4) => 0
  // (3, 2, 4) => 0
  // (4, 2, 4) => 1
  // (5, 2, 4) => 1

  static std::size_t worker_id_to_scheduler_id(std::size_t id,
                                                  std::size_t n_scheduler,
                                                  std::size_t n_worker) {
    if (id < n_scheduler) {
      return id;
    }
    return (id - n_scheduler) / (n_worker / n_scheduler);
  }

  // assume the replication group size is 3 and we have partitions 0..8
  // the 1st coordinator has partition 0, 3, 6.
  // the 2nd coordinator has partition 1, 4, 7.
  // the 3rd coordinator has partition 2, 5, 8.
  // the function first maps all partition id to 0, 1, 2 and then use % hash to
  // assign each partition to a scheduler.

  static std::size_t
  partition_id_to_scheduler_id(std::size_t partition_id,
                                  std::size_t n_scheduler,
                                  std::size_t replica_group_size) {
    return partition_id / replica_group_size % n_scheduler;
  }

  static void read(const std::tuple<MetaDataType *, void *> &row, void *dest,
                   std::size_t size) {

    MetaDataType &tid = *std::get<0>(row);
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
  }


  static void add_to_map(std::vector<std::unique_ptr<SkewScheduleMap>> &schedule_maps,
                                    std::size_t scheduler_id, SkewRWKey &rw_key, 
                                    std::size_t n_workers, std::size_t txn_id, bool is_ycsb) {
                                     
    std::size_t tableId = rw_key.get_table_id();
    const std::string &key = rw_key.key_toString(is_ycsb);
    
    if (schedule_maps[scheduler_id]->map_.find(key) == schedule_maps[scheduler_id]->map_.end()){
      
        // LOG(INFO)<< "T" << txn_id << "; scheduler_id:" << scheduler_id << "; key:(" 
        //                << rw_key.key_toString(is_ycsb) << ") not exists in schedule_map." << std::endl;
      
        schedule_maps[scheduler_id]->map_[key] = std::make_unique<SkewVector>();
        schedule_maps[scheduler_id]->map_[key]->cond_var_flags.resize(n_workers);
    }
    std::shared_ptr<SkewKeyInfo> key_info = std::make_shared<SkewKeyInfo>(txn_id, rw_key.is_write_key());
    std::unique_ptr<SkewVector> &skew_v = schedule_maps[scheduler_id]->map_[key];
    skew_v->pushBack(key_info);
  }

  static void initAllWriteIndex(std::vector<std::unique_ptr<SkewScheduleMap>> &schedule_maps,
                                std::size_t scheduler_id) {
    // init read_count and current_write_index for the schedule_map.
    for (auto it = schedule_maps[scheduler_id]->map_.begin(); it != schedule_maps[scheduler_id]->map_.end(); ++it) {
      it->second->initWriteIndex();
    } 
  }
};
} // namespace aria