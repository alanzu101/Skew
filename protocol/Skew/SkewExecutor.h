///
// Created by Yi Lu
// Modified by Alan Zu
//

#pragma once

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Skew/SkewHelper.h"
#include "protocol/Skew/SkewMessage.h"
#include "protocol/Skew/SkewPartitioner.h"
#include "protocol/Skew/SkewScheduleMap.h"
#include <chrono>
#include <thread>

using namespace std::chrono_literals;

namespace aria {

template <class Workload> class SkewExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = SkewTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = SkewMessage;
  using MessageFactoryType = SkewMessageFactory;
  using MessageHandlerType = SkewMessageHandler;

  SkewExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                 const ContextType &context,
                 std::vector<std::unique_ptr<TransactionType>> &transactions,
                 std::vector<StorageType> &storages,
                 std::atomic<uint32_t> &scheduler_status,
                 std::atomic<uint32_t> &worker_status,
                 std::atomic<uint32_t> &n_complete_workers,
                 std::atomic<uint32_t> &n_started_workers,
                 std::size_t &n_scheduler,  
                 std::vector<std::unique_ptr<SkewScheduleMap>> &schedule_maps,
                 std::atomic<uint32_t> &batch_count)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages),
        scheduler_status(scheduler_status), worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(coordinator_id, context.coordinator_num,
                    SkewHelper::string_to_vint(context.replica_group)),
        workload(coordinator_id, db, random, partitioner),
        n_scheduler(n_scheduler),
        n_workers(context.worker_num - n_scheduler),
        schedule_maps(schedule_maps),
        scheduler_id(SkewHelper::worker_id_to_scheduler_id(
            id, n_scheduler, n_workers)),
        batch_count(batch_count), init_transaction(false),
        random(id), // make sure each worker has a different seed.
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    CHECK(n_workers > 0 && n_workers % n_scheduler == 0);

    // init execute_flags
    execute_flags.resize(transactions.size());
    for (auto i = 0u; i < transactions.size(); i++) {
      execute_flags[i] = false;
    }

    is_ycsb = context.is_ycsb;
  }

  ~SkewExecutor() = default;

  void start() override {
    LOG(INFO) << "SkewExecutor " << id << " started. ";

    for (;;) {
      
      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "SkewExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Analysis);

      n_started_workers.fetch_add(1);
      generate_transactions();
      n_complete_workers.fetch_add(1);

      // wait to Execute

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Analysis) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      // work as scheduler
      if (id < n_scheduler) {
        // schedule transactions
        // LOG(INFO) << " scheduler:" << id << "; start to schedule..." << std::endl;
        schedule_transactions();
      } else {
        // work as executor
        run_transactions();
      }

      n_complete_workers.fetch_add(1);

      // wait to Analysis

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Execute) {
        process_msg_request();
      }
    }
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();

    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                message->time)
              .count() < delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

  void flush_messages() {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages[i].release();

      out_queue.push(message);
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

  void generate_transactions() {
    if (!context.same_batch || !init_transaction) {
      init_transaction = true;
      for (auto i = id; i < transactions.size(); i += context.worker_num) {
        // generate transaction
        auto partition_id = random.uniform_dist(0, context.partition_num - 1);
        transactions[i] =
            workload.next_transaction(context, partition_id, storages[i]);
        transactions[i]->set_id(i);
        prepare_transaction(*transactions[i]);
      }
    } else {
      auto now = std::chrono::steady_clock::now();
      for (auto i = id; i < transactions.size(); i += context.worker_num) {
        // a soft reset
        transactions[i]->network_size.store(0);
        transactions[i]->load_read_count();
        transactions[i]->clear_execution_bit();
        transactions[i]->startTime = now;
      }
    }
  }

  void prepare_transaction(TransactionType &txn) {

    setup_prepare_handlers(txn);
    // run txn.execute() to prepare read/write set
    // LOG(INFO) << "--- worker(" << id << ") is preparing T" << txn.id << " for read/write set." << std::endl;
    
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY) {
      txn.abort_no_retry = true;
    }

    if (context.same_batch) {
      txn.save_read_count();
    }

    analyze_active_coordinator(txn);

    // setup handlers for execution
    setup_execute_handlers(txn);
    txn.execution_phase = true;
  }

  void analyze_active_coordinator(TransactionType &transaction) {

    // assuming no blind write
    auto &readSet = transaction.readSet;
    auto &active_coordinators = transaction.active_coordinators;
    active_coordinators =
        std::vector<bool>(partitioner.total_coordinators(), false);

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readkey = readSet[i];
      if (readkey.get_local_index_read_bit()) {
        continue;
      }
      auto partitionID = readkey.get_partition_id();
      if (readkey.is_read_for_write_key()) {
        active_coordinators[partitioner.master_coordinator(partitionID)] = true;
      }
    }
  }

  void schedule_transactions() {

    std::size_t request_id = 0;

    // build up the schedule_maps[id] for this partitions[id], 
    // then assign the transaction to a worker thread in a round-robin manner.
    if (batch_count.load() == 0 || !context.same_batch) {
      execute_flags.clear();
      execute_flags.resize(transactions.size());

      // build up the schedule_maps[scheduler_id]
      for (auto i = 0u; i < transactions.size(); i++) {
        execute_flags[i] = false;

        // don't add abort no retry transaction into the schedule_maps[scheduler_id]
        if (!transactions[i]->abort_no_retry) {
          auto &rw_keys = transactions[i]->rw_SkewRWKeys;

          /*
          LOG(INFO)<< "T" << i << "; scheduler_id:" << scheduler_id << "; rw_SkewRWKeys:" << std::endl;
          for (auto it = rw_keys.begin(); it != rw_keys.end(); ++it)
            LOG(INFO) << "  table_id : " << (*it).get_table_id() << "; partition_id : " << (*it).get_partition_id() 
                        << "; key : (" << (*it).key_toString(is_ycsb) << ")" << std::endl;
          */

          for (auto k = 0u; k < rw_keys.size(); k++) {
            auto &rw_key = rw_keys[k];
            auto tableId = rw_key.get_table_id();
            auto partitionId = rw_key.get_partition_id();

            if (!partitioner.has_master_partition(partitionId)) {
              /*
              LOG(INFO) << "T" << i << "; scheduler_id:" << scheduler_id << "; partitionId:" 
                                                << partitionId << "is not the master partition.";
              LOG(INFO) << "T" << i << "; don't add this key:(" << rw_key.key_toString(is_ycsb) << ") into scheduler map.";
              */
              continue;
            }

            if (rw_key.get_local_index_read_bit()) {
              
              // LOG(INFO)<< "T" << i << "; scheduler_id:" << scheduler_id << "; key:(" 
              //                                << rw_key.key_toString(is_ycsb) << ") is local index read.";
              
              continue;
            }

            if (SkewHelper::partition_id_to_scheduler_id(
                  partitionId, n_scheduler, partitioner.replica_group_size) != scheduler_id) {
              /*   
              std::size_t expected_scheduler_id = SkewHelper::partition_id_to_scheduler_id(
                            partitionId, n_scheduler, partitioner.replica_group_size);
              LOG(INFO)<< "Build schedule_maps[" << scheduler_id << "] : T" << i << "; key:(" << rw_key.key_toString(is_ycsb) 
                          << "); expected_sheduler_id:" << expected_scheduler_id << std::endl;
              */
              continue;
            }

            execute_flags[i] = true;

            SkewHelper::add_to_map(schedule_maps, scheduler_id, rw_key, context.worker_num, i, is_ycsb);

          }
            // LOG(INFO)<<"scheduler_id:" << scheduler_id << "; done with T" << i << ". " << std::endl;
        }  
      }

      // LOG(INFO)<<"scheduler(" << scheduler_id << "): the schedule_maps[" << scheduler_id 
      //                                  << "] has been built up." << std::endl;
    }

    // init read_count and current_write_index for all the maps for this scheduler.
    SkewHelper::initAllWriteIndex(schedule_maps, scheduler_id);

    // assign the need-to-be-executed transactions within this partition to the local workers' threads.
    for (auto i = 0u; i < transactions.size(); i++) {
      // do not execute abort no retry transaction
      if (!transactions[i]->abort_no_retry) {
        // LOG(INFO)<<"scheduler_id = " << scheduler_id << "; execute_flags["<<i<<"] = " << execute_flags[i];
        
        if (execute_flags[i]) {
          auto worker = get_available_worker(request_id++);
          // LOG(INFO)<<"scheduler(" << scheduler_id << ") assigns worker(" << worker << ") for T" << i << std::endl;
          all_executors[worker]->transaction_queue.push(transactions[i].get());
        }
        // only count once
        if (i % n_scheduler == id) {
          n_commit.fetch_add(1);
        }
      } else {
        // only count once
        if (i % n_scheduler == id) {
          n_abort_no_retry.fetch_add(1);
        }
      }
    }
    set_scheduler_bit(id);
  }

  void run_transactions() {

    while (!get_scheduler_bit(scheduler_id) || !transaction_queue.empty()) {

      if (transaction_queue.empty()) {
        process_msg_request();
        continue;
      }

      TransactionType *transaction = transaction_queue.front();
      bool ok = transaction_queue.pop();
      DCHECK(ok);

      auto result = transaction->execute(id);
      n_network_size.fetch_add(transaction->network_size.load());
    
      auto latency =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - transaction->startTime)
              .count();
      percentile.add(latency);
    }
  }

  void setup_execute_handlers(TransactionType &txn) {
    txn.read_handler = [this, &txn](std::size_t worker_id, std::size_t id,
                                    uint32_t key_index, SkewRWKey &rw_key) {

      auto *worker = this->all_executors[worker_id];
      std::size_t table_id = rw_key.get_table_id();
      std::size_t partition_id = rw_key.get_partition_id();
      const void *key = rw_key.get_key();
      void *value = rw_key.get_value();
      if (worker->partitioner.has_master_partition(partition_id)) {
        
        std::unique_ptr<SkewVector> &skew_v = worker->schedule_maps[worker->scheduler_id]->getSkewVector(rw_key);
        for(;;) {
          std::shared_ptr<SkewKeyInfo> &cur_write_key_info = skew_v->getCurWriteKeyInfo();
          if (txn.id <= cur_write_key_info->getTransactionId()) {
            /*
            LOG(INFO) << "In read_handler(), worker(" << worker_id <<") on T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); maps[" << worker->scheduler_id << "]: ("
                  << cur_write_key_info->getTransactionId() << ":" << ((cur_write_key_info->isWriteKey())? "W" : "R") <<"); read_count:" << skew_v->getReadCount() << " ====> execute!" << std::endl;
            */
            break;
          } else {
          
            std::unique_lock<std::mutex> lock(skew_v->mutex);    
            cur_write_key_info = skew_v->getCurWriteKeyInfo();
            if (txn.id <= cur_write_key_info->getTransactionId()) {
              break;
            }
            
            skew_v->cond_var_flags[worker_id] = false;
            /*
            LOG(INFO) << "In read_handler(), worker(" << worker_id <<") on T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); maps[" << worker->scheduler_id << "]: ("
                  << cur_write_key_info->getTransactionId() << ":" << ((cur_write_key_info->isWriteKey())? "W" : "R") <<"); read_count:" << skew_v->getReadCount() 
                  << "; flag=" << skew_v->cond_var_flags[worker_id] <<";  ====> wait... " << std::endl;
            */

            skew_v->cond_var.wait(lock, [&skew_v, worker_id](){ return skew_v->cond_var_flags[worker_id]; });
            /*
            LOG(INFO) << "In read_handler(), worker(" << worker_id <<") on T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); maps[" << worker->scheduler_id << "]: ("
                  << cur_write_key_info->getTransactionId() << ":" << ((cur_write_key_info->isWriteKey())? "W" : "R") <<"); read_count:" << skew_v->getReadCount() 
                  << ";flag=" << skew_v->cond_var_flags[worker_id] <<";  ====> woke up! " << std::endl;
            */ 
          }
        }

        ITable *table = worker->db.find_table(table_id, partition_id);
        SkewHelper::read(table->search(key), value, table->value_size());

        skew_v->read_count.fetch_sub(1);
        if (skew_v->read_count.load() == 0) {
          // wake up all waiting threads.
          std::lock_guard<std::mutex> lock(skew_v->mutex);
          std::fill(skew_v->cond_var_flags.begin(), skew_v->cond_var_flags.end(), true);
          skew_v->cond_var.notify_all();
          /*
          LOG(INFO) << "In read_handler(), worker(" << worker_id <<") has done T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); current_write_index = " << skew_v->getWriteIndex() 
                  << "; read_count = " << skew_v->getReadCount() << "; flag=" << skew_v->cond_var_flags[worker_id] <<"; notify_all." << std::endl;
          */
        }

        auto &active_coordinators = txn.active_coordinators;
        for (auto i = 0u; i < active_coordinators.size(); i++) {
          if (i == worker->coordinator_id || !active_coordinators[i])
            continue;
          auto sz = MessageFactoryType::new_read_message(
              *worker->messages[i], *table, id, key_index, value);
          txn.network_size.fetch_add(sz);
          txn.distributed_transaction = true;
        }
        txn.local_read.fetch_add(-1);
      }
    };

    txn.write_handler = [this, &txn](std::size_t worker_id, std::size_t id,
                                    uint32_t key_index, SkewRWKey &rw_key) {
      auto *worker = this->all_executors[worker_id];
      std::size_t table_id = rw_key.get_table_id();
      std::size_t partition_id = rw_key.get_partition_id();
      const void *key = rw_key.get_key();
      void *value = rw_key.get_value();
      if (worker->partitioner.has_master_partition(partition_id)) {

        ITable *table = worker->db.find_table(table_id, partition_id);

        // check the schedule_map, make sure this transaction is the current T on the key.
        std::unique_ptr<SkewVector> &skew_v = worker->schedule_maps[worker->scheduler_id]->getSkewVector(rw_key);;
        for(;;) {
          std::shared_ptr<SkewKeyInfo> &cur_write_key_info = skew_v->getCurWriteKeyInfo();
          if (cur_write_key_info->getTransactionId() == txn.id && skew_v->getReadCount() == 0) {
          /*  
            LOG(INFO) << "In write_handler(), worker(" << worker_id <<") on T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); maps[" << worker->scheduler_id << "]: ("
                  << cur_write_key_info->getTransactionId() << ":" << ((cur_write_key_info->isWriteKey())? "W" : "R") <<"); read_count:" << skew_v->getReadCount() << "  ====> execute!" << std::endl;
          */        

            break;
          } else if(cur_write_key_info->getTransactionId() < txn.id || skew_v->getReadCount() > 0) {

            std::unique_lock<std::mutex> lock(skew_v->mutex);
            cur_write_key_info = skew_v->getCurWriteKeyInfo();
            if (cur_write_key_info->getTransactionId() == txn.id && skew_v->getReadCount() == 0) {
              break;
            }

            skew_v->cond_var_flags[worker_id] = false;
            /*
            LOG(INFO) << "In write_handler(), worker(" << worker_id <<") on T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); maps[" << worker->scheduler_id << "]: ("
                  << cur_write_key_info->getTransactionId() << ":" << ((cur_write_key_info->isWriteKey())? "W" : "R") <<"); read_count:" << skew_v->getReadCount() 
                  << "; flag=" << skew_v->cond_var_flags[worker_id] <<"; ====> wait... " << std::endl;
            */
            
            skew_v->cond_var.wait(lock, [&skew_v, worker_id](){ return skew_v->cond_var_flags[worker_id]; });
            /*
            LOG(INFO) << "In write_handler(), worker(" << worker_id <<") on T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); maps[" << worker->scheduler_id << "]: ("
                  << cur_write_key_info->getTransactionId() << ":" << ((cur_write_key_info->isWriteKey())? "W" : "R") <<"); read_count:" << skew_v->getReadCount() 
                  << ";flag=" << skew_v->cond_var_flags[worker_id] <<";  ====> woke up! " << std::endl; 
            */  
            
          } else if (cur_write_key_info->getTransactionId() > txn.id) {   // this txn should have been processed already.
            
            LOG(INFO) << "Error: In write_handler(), worker(" << worker_id <<") on T" << txn.id << ":key(" 
                  << rw_key.key_toString(is_ycsb) << "); maps[" << worker->scheduler_id << "]: ("
                  << cur_write_key_info->getTransactionId() << ":" << ((cur_write_key_info->isWriteKey())? "W" : "R") <<"); read_count:" << skew_v->getReadCount() << " ====> should never happen!" << std::endl;  
          }
        }

        table->update(key, value);

        {
          // wake up all waiting threads.
          std::lock_guard<std::mutex> lock(skew_v->mutex); 
          skew_v->nextWriteIndex();
          std::fill(skew_v->cond_var_flags.begin(),skew_v->cond_var_flags.end(), true);
          skew_v->cond_var.notify_all();
          /*
          LOG(INFO) << "In write_handler(), worker(" << worker_id <<") has done T" << txn.id << ":key(" 
                    <<  rw_key.key_toString(is_ycsb) << "); current_write_index = " << skew_v->getWriteIndex() 
                    << "; read_count = " << skew_v->getReadCount() << "; notify_all." << std::endl;
          */
        }
      }
    };

    txn.setup_process_requests_in_execution_phase(
        n_scheduler, n_workers, partitioner.replica_group_size);
    txn.remote_request_handler = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      return worker->process_msg_request();
    };
    txn.message_flusher = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      worker->flush_messages();
    };
  };

  void setup_prepare_handlers(TransactionType &txn) {
    txn.local_index_read_handler = [this](std::size_t table_id,
                                          std::size_t partition_id,
                                          const void *key, void *value) {
      ITable *table = this->db.find_table(table_id, partition_id);
      SkewHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  };

  void set_all_executors(const std::vector<SkewExecutor *> &executors) {
    all_executors = executors;
  }

  std::size_t get_available_worker(std::size_t request_id) {
    // assume there are n schedulers and m workers
    // 0, 1, .. n-1 are schedulers
    // n, n + 1, .., n + m -1 are workers

    // the first schedulers assign transactions to n, .. , n + m/n - 1

    auto start_worker_id = n_scheduler + n_workers / n_scheduler * id;
    auto len = n_workers / n_scheduler;
    return request_id % len + start_worker_id;
  }

  void set_scheduler_bit(int id) {
    uint32_t old_value, new_value;
    do {
      old_value = scheduler_status.load();
      DCHECK(((old_value >> id) & 1) == 0);
      new_value = old_value | (1 << id);
    } while (!scheduler_status.compare_exchange_weak(old_value, new_value));
  }

  bool get_scheduler_bit(int id) {
    return (scheduler_status.load() >> id) & 1;
  }

  std::size_t process_msg_request() {

    std::size_t size = 0;

    while (!in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool ok = in_queue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        ITable *table = db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());
        messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], *table,
                              transactions);
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &scheduler_status, &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  SkewPartitioner partitioner;
  WorkloadType workload;
  std::size_t &n_scheduler, n_workers;
  std::vector<std::unique_ptr<SkewScheduleMap>> &schedule_maps;
  std::size_t scheduler_id;
  std::size_t request_id;
  std::atomic<uint32_t> &batch_count;

  // flag the transaction if need to be executed or not.
  std::vector<bool> execute_flags;     

  bool init_transaction;
  RandomType random;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
  LockfreeQueue<TransactionType *> transaction_queue;
  std::vector<SkewExecutor *> all_executors;
  bool is_ycsb = false;
};
} // namespace aria