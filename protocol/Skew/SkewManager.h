//
// Created by Yi Lu
// Modified by Alan Zu
//

#pragma once

#include "core/Manager.h"
#include "protocol/Skew/SkewExecutor.h"
#include "protocol/Skew/SkewHelper.h"
#include "protocol/Skew/SkewPartitioner.h"
#include "protocol/Skew/SkewTransaction.h"
#include "protocol/Skew/SkewScheduleMap.h"

#include <thread>
#include <vector>

namespace aria {

template <class Workload> class SkewManager : public aria::Manager {
public:
  using base_type = aria::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SkewTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  SkewManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db),
        partitioner(coordinator_id, context.coordinator_num,
                    SkewHelper::string_to_vint(context.replica_group)),
        n_scheduler(SkewHelper::n_scheduler(partitioner.replica_group_id, id,
            SkewHelper::string_to_vint(context.scheduler))) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
    schedule_maps.resize(n_scheduler);

    // LOG(INFO)<<"context.scheduler = " << context.scheduler <<std::endl;
    // LOG(INFO)<<"context.replica_group = "<< context.replica_group <<std::endl;
    // LOG(INFO)<<"n_scheduler = " << n_scheduler << "; partitioner.replica_group_id = " << partitioner.replica_group_id<<std::endl;
    // LOG(INFO)<<"partitioner.replica_group_size = "<< partitioner.replica_group_size <<std::endl;
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;
    
    batch_count.store(0);
    while (!stopFlag.load()) {

      // LOG(INFO)<< "---- manager(" << this->id << ") is signaling to start T batch : " << batch_count.load() << std::endl;

      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.
      // LOG(INFO) << "Seed: " << random.get_seed();

      // initialize schedule_maps 
      if (batch_count.load() == 0 || !context.same_batch) {
        for (auto scheduler_id = 0u; scheduler_id < n_scheduler; scheduler_id++) {
          schedule_maps[scheduler_id] = ScheduleMapFactory::create_schedule_map();
        }
      }
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Analysis);
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction
      wait_all_workers_start();
      wait_all_workers_finish();

      // wait for all machines until they finish the analysis phase.
      wait4_ack();

      // LOG(INFO)<< "---- manager(" << this->id << ") is signaling to enter EXECUTE_PHASE for T batch : " << batch_count << std::endl;

      // Allow each worker to run transactions upon assignment via the queue.
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_scheduler_status();
      signal_worker(ExecutorStatus::Execute);
      wait_all_workers_start();
      wait_all_workers_finish();
      // wait for all machines until they finish the execution phase.
      wait4_ack();

      batch_count++;
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;
  
    batch_count.store(0);
    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();

      // initialize schedule_maps 
      if (batch_count.load() == 0 || !context.same_batch) {
        for (auto scheduler_id = 0u; scheduler_id < n_scheduler; scheduler_id++) {
          schedule_maps[scheduler_id] = ScheduleMapFactory::create_schedule_map();
        }
      }
      
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::Analysis);
      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Analysis);
      wait_all_workers_start();
      wait_all_workers_finish();

      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::Execute);
      // Allow each worker to run transactions upon assignment via the queue.
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_scheduler_status();
      set_worker_status(ExecutorStatus::Execute);
      wait_all_workers_start();
      wait_all_workers_finish();
      send_ack();

      batch_count.fetch_add(1);
    }
  }

  void add_worker(const std::shared_ptr<SkewExecutor<WorkloadType>> &w) {
    workers.push_back(w);
  }

  void clear_scheduler_status() { scheduler_status.store(0); }

public:
  RandomType random;
  DatabaseType &db;
  SkewPartitioner partitioner;
  std::atomic<uint32_t> scheduler_status;
  std::vector<std::shared_ptr<SkewExecutor<WorkloadType>>> workers;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::size_t n_scheduler;
  std::vector<std::unique_ptr<SkewScheduleMap>> schedule_maps;
  std::atomic<uint32_t> batch_count;
};
} // namespace aria