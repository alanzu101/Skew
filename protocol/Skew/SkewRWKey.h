//
// Created by Yi Lu on 9/14/18.
// Modified by Alan Zu on 2/28/2023.
//

#pragma once

#include <glog/logging.h>
#include <sstream>

namespace aria {

class SkewRWKey {
public:
  // local index read bit

  void set_local_index_read_bit() {
    clear_local_index_read_bit();
    bitvec |= LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET;
  }

  void clear_local_index_read_bit() {
    bitvec &= ~(LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET);
  }

  uint32_t get_local_index_read_bit() const {
    return (bitvec >> LOCAL_INDEX_READ_BIT_OFFSET) & LOCAL_INDEX_READ_BIT_MASK;
  }

  // read_for_write_key bit
  void set_read_for_write_key_bit() {
    clear_read_for_write_key_bit();
    bitvec |= READ_FOR_WRITE_KEY_BIT_MASK << READ_FOR_WRITE_KEY_BIT_OFFSET;
  }

  void clear_read_for_write_key_bit() {
    bitvec &= ~(READ_FOR_WRITE_KEY_BIT_MASK << READ_FOR_WRITE_KEY_BIT_OFFSET);
  }

  uint32_t get_read_for_write_key_bit() const {
    return (bitvec >> READ_FOR_WRITE_KEY_BIT_OFFSET) & READ_FOR_WRITE_KEY_BIT_MASK;
  }

  bool is_read_for_write_key() {
    return bitvec & (READ_FOR_WRITE_KEY_BIT_MASK << READ_FOR_WRITE_KEY_BIT_OFFSET);
  }

  // write_key bit
  void set_write_key_bit() {
    clear_write_key_bit();
    bitvec |= WRITE_KEY_BIT_MASK << WRITE_KEY_BIT_OFFSET;
  }

  void clear_write_key_bit() {
    bitvec &= ~(WRITE_KEY_BIT_MASK << WRITE_KEY_BIT_OFFSET);
  }

  uint32_t get_write_key_bit() const {
    return (bitvec >> WRITE_KEY_BIT_OFFSET) & WRITE_KEY_BIT_MASK;
  }

  bool is_write_key() {
    return bitvec & (WRITE_KEY_BIT_MASK << WRITE_KEY_BIT_OFFSET);
  }

  // prepare processed bit

  void set_prepare_processed_bit() {
    clear_prepare_processed_bit();
    bitvec |= PREPARE_PROCESSED_BIT_MASK << PREPARE_PROCESSED_BIT_OFFSET;
  }

  void clear_prepare_processed_bit() {
    bitvec &= ~(PREPARE_PROCESSED_BIT_MASK << PREPARE_PROCESSED_BIT_OFFSET);
  }

  uint32_t get_prepare_processed_bit() const {
    return (bitvec >> PREPARE_PROCESSED_BIT_OFFSET) &
           PREPARE_PROCESSED_BIT_MASK;
  }

  // execution processed bit

  void set_execution_processed_bit() {
    clear_execution_processed_bit();
    bitvec |= EXECUTION_PROCESSED_BIT_MASK << EXECUTION_PROCESSED_BIT_OFFSET;
  }

  void clear_execution_processed_bit() {
    bitvec &= ~(EXECUTION_PROCESSED_BIT_MASK << EXECUTION_PROCESSED_BIT_OFFSET);
  }

  uint32_t get_execution_processed_bit() const {
    return (bitvec >> EXECUTION_PROCESSED_BIT_OFFSET) &
           EXECUTION_PROCESSED_BIT_MASK;
  }

  // table id

  void set_table_id(uint32_t table_id) {
    DCHECK(table_id < (1 << 5));
    clear_table_id();
    bitvec |= table_id << TABLE_ID_OFFSET;
  }

  void clear_table_id() { bitvec &= ~(TABLE_ID_MASK << TABLE_ID_OFFSET); }

  uint32_t get_table_id() const {
    return (bitvec >> TABLE_ID_OFFSET) & TABLE_ID_MASK;
  }
  // partition id

  void set_partition_id(uint32_t partition_id) {
    DCHECK(partition_id < (1 << 16));
    clear_partition_id();
    bitvec |= partition_id << PARTITION_ID_OFFSET;
  }

  void clear_partition_id() {
    bitvec &= ~(PARTITION_ID_MASK << PARTITION_ID_OFFSET);
  }

  uint32_t get_partition_id() const {
    return (bitvec >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK;
  }

  // key
  void set_key(const void *key) { this->key = key; }

  const void *get_key() const { return key; }

  // value
  void set_value(void *value) { this->value = value; }

  void *get_value() const { return value; }

  const std::string &key_toString(bool is_ycsb) {
    if (str_key != "") {
      return str_key;
    }
    std::stringstream ss;
    size_t tableId = get_table_id();

    // for ycsb
    if (is_ycsb && tableId == ycsb::ycsb::tableID) {
      const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
      ss << k.Y_KEY;
      str_key = ss.str();
      return str_key;
    }

    // for tpcc
    if (tableId == tpcc::warehouse::tableID) {
      const auto &k = *static_cast<const tpcc::warehouse::key *>(key);
      ss << tableId << ":" << k.W_ID;
    } else if (tableId == tpcc::district::tableID) {
      const auto &k = *static_cast<const tpcc::district::key *>(key);
      ss << tableId << ":" << k.D_W_ID << ":" << k.D_ID;
    } else if (tableId == tpcc::customer::tableID) {
      const auto &k = *static_cast<const tpcc::customer::key *>(key);
      ss << tableId << ":" << k.C_W_ID << ":" << k.C_D_ID << ":" << k.C_ID; 
    } else if (tableId == tpcc::item::tableID) {
      const auto &k = *static_cast<const tpcc::item::key *>(key);
      ss << tableId << ":" << k.I_ID; 
    } else if (tableId == tpcc::stock::tableID) {
      const auto &k = *static_cast<const tpcc::stock::key *>(key);
      ss << tableId << ":" << k.S_W_ID << ":" << k.S_I_ID; 
    } else if (tableId == tpcc::new_order::tableID) {
      const auto &k = *static_cast<const tpcc::new_order::key *>(key);
      ss << tableId << ":" << k.NO_W_ID << ":" << k.NO_D_ID << ":" << k.NO_O_ID; 
    } else if (tableId == tpcc::order::tableID) {
      const auto &k = *static_cast<const tpcc::order::key *>(key);
      ss << tableId << ":" << k.O_W_ID << ":" << k.O_D_ID << ":" << k.O_ID; 
    } else if (tableId == tpcc::order_line::tableID) {
      const auto &k = *static_cast<const tpcc::order_line::key *>(key);
      ss << tableId << ":" << k.OL_W_ID << ":" << k.OL_D_ID << ":" << k.OL_O_ID << ":" << k.OL_NUMBER; 
    } else {
      LOG(INFO)<<"**** Error in key type : key_toString().";
    }
    str_key = ss.str();
    return str_key;
  }

private:
  /*
   * A bitvec is a 32-bit word.
   *
   * [ table id (5) ] | partition id (16) | unused bit (6) |
   * prepare processed bit (1) | execute processed bit(1) |
   * write key bit(1) | read_for_write key bit (1) | local index read (1)  ]
   *
   * local index read  is set when the read is from a local read only index.
   * write key bit is set when the key is write key.
   * read_for_write key bit is set when the key is read_for_write key.
   * prepare processed bit is set when process_request has processed this key in
   * prepare phase exucution processed bit is set when process_request has
   * processed this key in execution phase
   */

  uint32_t bitvec = 0;
  const void *key = nullptr;
  void *value = nullptr;
  std::string str_key = "";

public:
  static constexpr uint32_t TABLE_ID_MASK = 0x1f;
  static constexpr uint32_t TABLE_ID_OFFSET = 27;

  static constexpr uint32_t PARTITION_ID_MASK = 0xffff;
  static constexpr uint32_t PARTITION_ID_OFFSET = 11;

  static constexpr uint32_t EXECUTION_PROCESSED_BIT_MASK = 0x1;
  static constexpr uint32_t EXECUTION_PROCESSED_BIT_OFFSET = 4;

  static constexpr uint32_t PREPARE_PROCESSED_BIT_MASK = 0x1;
  static constexpr uint32_t PREPARE_PROCESSED_BIT_OFFSET = 3;

  static constexpr uint32_t WRITE_KEY_BIT_MASK = 0x1;
  static constexpr uint32_t WRITE_KEY_BIT_OFFSET = 2;

  static constexpr uint32_t READ_FOR_WRITE_KEY_BIT_MASK = 0x1;
  static constexpr uint32_t READ_FOR_WRITE_KEY_BIT_OFFSET = 1;

  static constexpr uint32_t LOCAL_INDEX_READ_BIT_MASK = 0x1;
  static constexpr uint32_t LOCAL_INDEX_READ_BIT_OFFSET = 0;
};
} // namespace aria