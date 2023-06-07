//
// Created by Yi Lu
// Modified by Alan Zu
//

#pragma once

#include <chrono>
#include <glog/logging.h>
#include <thread>
#include "core/Defs.h"
#include "protocol/Skew/SkewHelper.h"
#include "protocol/Skew/SkewPartitioner.h"
#include "protocol/Skew/SkewRWKey.h"
#include "common/Operation.h"


namespace aria {

class SkewTransaction {

public:

  SkewTransaction(std::size_t coordinator_id, std::size_t partition_id,
                    Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~SkewTransaction() = default;

  void reset() {
    local_read.store(0);
    saved_local_read = 0;
    remote_read.store(0);
    saved_remote_read = 0;
    abort_no_retry = false;
    distributed_transaction = false;
    execution_phase = false;
    network_size.store(0);
    active_coordinators.clear();
    operation.clear();
    readSet.clear();
    writeSet.clear();
    rw_SkewRWKeys.clear();
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {

    if (execution_phase) {
      return;
    }

    SkewRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();

    add_to_read_set(readKey);

    // not involve in scheduling.
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {

    if (execution_phase) {
      return;
    }

    SkewRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    add_to_read_set(readKey);
    rw_SkewRWKeys.push_back(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    SkewRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_for_write_key_bit();

    add_to_read_set(readKey);
    rw_SkewRWKeys.push_back(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {

    if (execution_phase) {
      return;
    }

    SkewRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    writeKey.set_write_key_bit();

    add_to_write_set(writeKey);
    rw_SkewRWKeys.push_back(writeKey);
  }

  std::size_t add_to_read_set(const SkewRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const SkewRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(std::size_t id) { this->id = id; }

  void setup_process_requests_in_prepare_phase() {
    // process the reads in read-only index
    // for general reads, increment the local_read and remote_read counter.
    // the function may be called multiple times, the keys are processed in
    // reverse order.
    process_requests = [this](std::size_t worker_id) {
      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        // early return
        if (readSet[i].get_prepare_processed_bit()) {
          break;
        }

        if (readSet[i].get_local_index_read_bit()) {
          // this is a local index read
          auto &readKey = readSet[i];
          local_index_read_handler(readKey.get_table_id(),
                                   readKey.get_partition_id(),
                                   readKey.get_key(), readKey.get_value());
        } else {

          if (partitioner.has_master_partition(readSet[i].get_partition_id())) {
            local_read.fetch_add(1);
          } else {
            remote_read.fetch_add(1);
          }
        }

        readSet[i].set_prepare_processed_bit();
      }
      return false;
    };
  }

  void
  setup_process_requests_in_execution_phase(std::size_t n_scheduler,
                                            std::size_t n_worker,
                                            std::size_t replica_group_size) {
   
    process_requests = [this, n_scheduler, n_worker,
                        replica_group_size](std::size_t worker_id) {
      auto scheduler_id = SkewHelper::worker_id_to_scheduler_id(
          worker_id, n_scheduler, n_worker);

      // LOG(INFO) << "In process_requests(), worker(" << worker_id <<") starts on T" << this->id << std::endl;
      auto &rw_keys = this->rw_SkewRWKeys;
      uint32_t n_rw_keys = static_cast<unsigned int>(rw_keys.size());
      for (uint32_t k = 0u; k < n_rw_keys; k++) {
        auto &rw_key = rw_keys[k];
        if (rw_key.get_local_index_read_bit()) {
          continue;
        }

        if (SkewHelper::partition_id_to_scheduler_id(
                rw_key.get_partition_id(), n_scheduler,
                replica_group_size) != scheduler_id) {
          continue;
        }

        // early return
        if (rw_key.get_execution_processed_bit()) {
          break;
        }

        if (rw_key.is_write_key()) {
          write_handler(worker_id, this->id, k, rw_key);
        } else {
          read_handler(worker_id, this->id, k, rw_key);  
        }

        rw_key.set_execution_processed_bit();
      }

      message_flusher(worker_id);

      // LOG(INFO) << "In process_requests(), worker(" << worker_id <<") has done on T" << this->id << "; after message_flusher()" << std::endl;

      if (active_coordinators[coordinator_id]) {

        // spin on local & remote read
        while (local_read.load() > 0 || remote_read.load() > 0) {
          // process remote reads for other workers
          remote_request_handler(worker_id);
        }

        return false;
      } else {
        // abort if not active
        return true;
      }
    };
  }

  void save_read_count() {
    saved_local_read = local_read.load();
    saved_remote_read = remote_read.load();
  }

  void load_read_count() {
    local_read.store(saved_local_read);
    remote_read.store(saved_remote_read);
  }

  void clear_execution_bit() {
    for (auto i = 0u; i < readSet.size(); i++) {

      if (readSet[i].get_local_index_read_bit()) {
        continue;
      }

      readSet[i].clear_execution_processed_bit();
    }
  }

public:
  std::size_t coordinator_id, partition_id, id;
  std::chrono::steady_clock::time_point startTime;
  std::atomic<int32_t> network_size;
  std::atomic<int32_t> local_read, remote_read;
  int32_t saved_local_read, saved_remote_read;

  bool abort_no_retry;

  bool distributed_transaction;
  bool execution_phase;

  std::function<bool(std::size_t)> process_requests;

  // table id, partition id, key, value
  std::function<void(std::size_t, std::size_t, const void *, void *)>
      local_index_read_handler;

  // worker_id, txn_id, key_offset, rw_key
  std::function<void(std::size_t, std::size_t, uint32_t, SkewRWKey &)>
      read_handler;

  // worker_id, txn_id, key_offset, rw_key
  std::function<void(std::size_t, std::size_t, uint32_t, SkewRWKey &)>
      write_handler;

  // processed a request?
  std::function<std::size_t(std::size_t)> remote_request_handler;

  std::function<void(std::size_t)> message_flusher;

  Partitioner &partitioner;
  std::vector<bool> active_coordinators;
  Operation operation; // never used
  std::vector<SkewRWKey> readSet, writeSet;

  // for schedule_map's keys
  std::vector<SkewRWKey>  rw_SkewRWKeys;  
};
} // namespace aria