/*
 * Copyright 2017, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include <string>
#include <vector>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

using std::string;
using std::vector;
using nvml::obj::p;
using nvml::obj::persistent_ptr;
using nvml::obj::make_persistent;
using nvml::obj::transaction;
using nvml::obj::delete_persistent;
using nvml::obj::pool;

namespace pmemkv {

const string LAYOUT = "pmemkv";                            // unique pool layout identifier

#define INNER_KEYS 4                                       // maximum keys for inner nodes
#define INNER_KEYS_MIDPOINT (INNER_KEYS / 2)               // halfway point within the node
#define INNER_KEYS_UPPER ((INNER_KEYS / 2) + 1)            // index where upper half of keys begins
#define LEAF_KEYS 48                                       // maximum keys in tree nodes
#define LEAF_KEYS_MIDPOINT (LEAF_KEYS / 2)                 // halfway point within the node

class KVSlot {
  public:
    uint8_t hash() const { return ph; }                    // gets Pearson hash for key
    const char* key() const { return kv.get(); }           // gets key as c-style string
    const char* val() const { return kv.get() + ks + 1; }  // gets value as c-style string
    void clear();                                          // frees persistent memory
    void set(const uint8_t hash, const string& key,        // sets all slot fields
             const string& value);
  private:
    uint8_t ph;                                            // Pearson hash for key
    uint32_t ks;                                           // key size
    uint32_t vs;                                           // value size
    persistent_ptr<char[]> kv;                             // buffer for key & value
};

struct KVLeaf {
    p<KVSlot> slots[LEAF_KEYS];                            // array of slot containers
    persistent_ptr<KVLeaf> next;                           // next leaf in unsorted list
};

struct KVRoot {                                            // persistent root object
    persistent_ptr<KVLeaf> head;                           // head of linked list of leaves
};

struct KVNode {                                            // volatile nodes of the tree
    bool is_leaf = false;                                  // indicate inner or leaf node
    KVNode* parent;                                        // parent of this node (null if top)
};

struct KVInnerNode : KVNode {                              // volatile inner nodes of the tree
    uint8_t keycount;                                      // count of keys in this node
    string keys[INNER_KEYS + 1];                           // child keys plus one overflow slot
    KVNode* children[INNER_KEYS + 2];                      // child nodes plus one overflow slot
};

struct KVLeafNode : KVNode {                               // volatile leaf nodes of the tree
    uint8_t hashes[LEAF_KEYS];                             // Pearson hashes of keys
    string keys[LEAF_KEYS];                                // keys stored in this leaf
    persistent_ptr<KVLeaf> leaf;                           // pointer to persistent leaf
    bool lock;                                             // boolean modification lock
};

struct KVRecoveredLeaf {                                   // temporary wrapper used for recovery
    KVLeafNode* leafnode;                                  // leaf node being recovered
    char* max_key;                                         // highest sorting key present
};

enum KVStatus {                                            // status enumeration
    OK = 0,                                                // successful completion
    NOT_FOUND = 1,                                         // key not located
    TXN_ERROR = 2                                          // persistent transaction failed
};

struct KVTreeAnalysis {                                    // tree analysis structure
    size_t leaf_empty;                                     // count of persisted leaves w/o keys
    size_t leaf_prealloc;                                  // count of persisted but unused leaves
    size_t leaf_total;                                     // count of all persisted leaves
    string path;                                           // path when constructed
    size_t size;                                           // actual size of persistent pool
};

class KVTree {                                             // persistent tree class
  public:
    KVTree(const string& path, const size_t size);         // default constructor
    ~KVTree();                                             // default destructor
    void Analyze(KVTreeAnalysis& analysis);                // report on internal state & stats
    KVStatus Delete(const string& key);                    // remove single key
    KVStatus Get(const string& key, string* value);        // read single key/value
    vector<KVStatus> GetList(const vector<string>& keys,   // read multiple keys at once
                             vector<string>* values);
    KVStatus Put(const string& key, const string& value);  // write single key/value
  protected:
    KVLeafNode* LeafSearch(const string& key);             // find node for key
    void LeafFillEmptySlot(KVLeafNode* leafnode,           // write first unoccupied slot found
                           const uint8_t hash,
                           const string& key,
                           const string& value);
    bool LeafFillSlotForKey(KVLeafNode* leafnode,          // write slot for matching key if found
                            const uint8_t hash,
                            const string& key,
                            const string& value);
    void LeafFillSpecificSlot(KVLeafNode* leafnode,        // write slot at specific index
                              const uint8_t hash,
                              const string& key,
                              const string& value,
                              const int slot);
    void LeafSplitFull(KVLeafNode* leafnode,               // split full leaf into two leaves
                       const uint8_t hash,
                       const string& key,
                       const string& value);
    void InnerUpdateAfterSplit(KVNode* node,               // update parents after leaf split
                               KVNode* new_node,
                               string* split_key);
    uint8_t PearsonHash(const char* data,                  // calculate 1-byte hash for string
                        const size_t size);
    void Recover();                                        // reload state from persistent pool
    void Shutdown();                                       // release resources for tree
    void Shutdown(KVNode* node);                           // release resources for node
  private:
    KVTree(const KVTree&);                                 // prevent copying
    void operator=(const KVTree&);                         // prevent assignment
    vector<persistent_ptr<KVLeaf>> leaves_prealloc;        // persisted but unused leaves
    const string pmpath;                                   // path when constructed
    pool<KVRoot> pmpool;                                   // pool for persistent root
    size_t pmsize;                                         // actual size of persistent pool
    KVNode* tree_top = nullptr;                            // pointer to uppermost inner node
};

} // namespace pmemkv
