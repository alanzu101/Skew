//
// Created by Alan Zu
//

#pragma once

#include <mutex>
#include <thread>
#include <condition_variable>
#include <semaphore>
#include <unordered_map>
#include <variant>
#include "core/Context.h"

#include "protocol/Skew/SkewRWKey.h"


namespace aria {

class SkewKeyInfo {
public:
    SkewKeyInfo(uint32_t t_id, bool is_write) {
        this->transaction_id = t_id;
        this->is_write_key = is_write;
    }

    ~SkewKeyInfo() = default;

    uint32_t getTransactionId() {
        return transaction_id;
    }

    bool isWriteKey() {
        return is_write_key;
    }
private:
    uint32_t transaction_id;
    bool  is_write_key;
};    

class SkewVector {
public:
    SkewVector() {}

    ~SkewVector() = default;

    uint32_t getWriteIndex() {
        return current_write_index.load();
    }

    bool initWriteIndex() {
        current_write_index.store(0);
        read_count.store(0);
        if (current_write_index.load() == 0 && vec_[0]->isWriteKey()) {
            return true;
        }
        return nextWriteIndex();
    }

    bool nextWriteIndex() {
        read_count.store(0);
        if (current_write_index.load() == 0 && !(vec_[0]->isWriteKey())) {
            read_count.fetch_add(1);
        }

        auto limit = vec_.size()-1;
        while(current_write_index.load() < limit) {
            current_write_index.fetch_add(1);
            if (vec_[current_write_index.load()]->isWriteKey()) {
                return true;
            } else {
                read_count.fetch_add(1);
            }
        }
        // no more write.
        return false;
    }

    uint32_t getReadCount() {
        return read_count.load();
    }

    void subReadCount() {
        DCHECK(read_count.load() > 0);
        read_count.fetch_sub(1);
    }

    uint32_t getVectorSize() {
        return vec_.size();
    }

    std::shared_ptr<SkewKeyInfo> &getCurWriteKeyInfo() {
        //DCHECK(current_write_index < vec_.size());
        return vec_[current_write_index.load()];
    }

    void pushBack(std::shared_ptr<SkewKeyInfo> key_info) {
        vec_.push_back(key_info);
    }

    std::mutex mutex;
    std::condition_variable cond_var;
    std::vector<bool> cond_var_flags;
  
    std::atomic<uint32_t> read_count;
    std::atomic<uint32_t> current_write_index;
    std::vector<std::shared_ptr<SkewKeyInfo>> vec_;
};    


class SkewScheduleMap {
public:

    SkewScheduleMap() {
        aria::Context context;
        if (context.is_ycsb) {
            is_ycsb = true;
        }
    }
    ~SkewScheduleMap() = default;


    std::unique_ptr<SkewVector> &getSkewVector(SkewRWKey &rw_key) {
        const std::string &k = rw_key.key_toString(is_ycsb);
        return map_[k];        
    }   

    std::unordered_map<std::string, std::unique_ptr<SkewVector>> map_;
    bool is_ycsb = false;

};  

class ScheduleMapFactory {
public:

    static std::unique_ptr<SkewScheduleMap> create_schedule_map() {
        return std::make_unique<SkewScheduleMap>();
    }
};

}