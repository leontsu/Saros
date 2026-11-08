#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <set>
#include <sstream>
#include <thread>
#include <tuple>
#include <vector>

#include "common/debug.hh"
#include "workloads/eval_sampler.hh"
#include "common/heap_object.hh"
#include "common/queue.hh"
#include "common/random.hh"
#include "common/result.hh"
#include "common/tuple_body.hh"
#include "common/util.hh"
#include "common/workload.hh"

#include "gflags/gflags.h"

#ifdef GLOBAL_VALUE_DEFINE
DEFINE_uint32(bomb_mixed_short_rate, 500,
              "Short-request batches per minute, over all queues (per second with "
              "--bomb_mixed_short_rate_tps).");
DEFINE_bool(bomb_mixed_short_rate_tps, false,
            "Interpret --bomb_mixed_short_rate per second instead of per minute.");
DEFINE_bool(bomb_use_cache, false,
            "Static BoM: L1 walks a BoM tree cached at start-up instead of reading "
            "the BoM tables.");
DEFINE_uint64(bomb_perc_s1, 50, "Percentage of S1 (update material cost) requests.");
DEFINE_uint64(bomb_perc_s2, 50, "Percentage of S2 (issue journal voucher) requests.");
DEFINE_uint64(bomb_perc_s3, 0, "Percentage of S3 (change product) requests.");
DEFINE_uint64(bomb_perc_s4, 0, "Percentage of S4 (change raw material) requests; "
              "S5 (change product quantity) gets the remainder.");
DEFINE_uint32(bomb_req_batch_size, 1, "Requests pushed to a queue per dispatch.");
DEFINE_uint32(bomb_factory_size, 8, "Number of factories.");
DEFINE_uint32(bomb_product_size, 72000, "Number of finished products.");
DEFINE_uint32(bomb_work_size, 198000, "Number of root work-in-progress items.");
DEFINE_uint32(bomb_material_size, 75000, "Number of raw materials.");
DEFINE_uint32(bomb_tree_num_per_product, 5, "Root work-in-progress items per product.");
DEFINE_uint32(bomb_base_tree_size, 10, "Work-in-progress nodes per tree.");
DEFINE_uint32(bomb_material_per_wip, 3, "Raw materials per leaf work-in-progress node.");
DEFINE_uint32(bomb_base_product_size_per_factory, 100,
              "Products each factory manufactures (the products one L1 costs).");
DEFINE_uint32(bomb_mc_update_size, 1, "Materials one S1 updates.");
DEFINE_bool(bomb_mark_obsolete, false,
            "Obsolete-row BoMB: L1 rewrites the ProductCostMaster rows of products no "
            "longer in ItemManufacturingMaster as obsolete, and S2 skips such rows.");
#else
DECLARE_uint32(bomb_mixed_short_rate);
DECLARE_bool(bomb_mixed_short_rate_tps);
DECLARE_bool(bomb_use_cache);
DECLARE_uint64(bomb_perc_s1);
DECLARE_uint64(bomb_perc_s2);
DECLARE_uint64(bomb_perc_s3);
DECLARE_uint64(bomb_perc_s4);
DECLARE_uint32(bomb_req_batch_size);
DECLARE_uint32(bomb_factory_size);
DECLARE_uint32(bomb_product_size);
DECLARE_uint32(bomb_work_size);
DECLARE_uint32(bomb_material_size);
DECLARE_uint32(bomb_tree_num_per_product);
DECLARE_uint32(bomb_base_tree_size);
DECLARE_uint32(bomb_material_per_wip);
DECLARE_uint32(bomb_base_product_size_per_factory);
DECLARE_uint32(bomb_mc_update_size);
DECLARE_bool(bomb_mark_obsolete);
#endif

typedef std::chrono::high_resolution_clock::time_point timepoint;

// Thread 0 runs L1 back to back; every other worker serves one request queue.
constexpr uint32_t kL1ThreadNum = 1;

GLOBAL std::atomic<uint32_t> ItemIdCounter;
GLOBAL ConcurrentQueue<std::pair<TxType, timepoint>> *requestQueues;

enum class Storage : std::uint32_t {
  ItemConstructionMaster = 0,
  ItemManufacturingMaster,
  MaterialCostMaster,
  ProductCostMaster,
  JournalVoucher,
  Size,
};

// L1, S1, S2, S3, S4 and S5 of the paper, in this order.
enum class TxType : std::uint32_t {
  UpdateProductCostMaster = 0,
  UpdateMaterialCostMaster,
  IssueJournalVoucher,
  AddNewProduct,
  ChangeRawMaterial,
  ChangeProductQuantity,
  Size,
};

struct ItemConstructionMaster {
  alignas(CACHE_LINE_SIZE)
  std::uint32_t ic_parent_i_id;
  std::uint32_t ic_i_id;
  double ic_material_quantity;

  static void CreateKey(uint32_t ic_parent_i_id, uint32_t ic_i_id, char *out) {
    assign_as_bigendian(ic_parent_i_id, &out[0]);
    assign_as_bigendian(ic_i_id, &out[4]);
  }
};

struct MaterialCostMaster {
  alignas(CACHE_LINE_SIZE)
  std::uint32_t mc_f_id;
  std::uint32_t mc_i_id;
  double mc_stock_quantity;
  double mc_stock_price;

  static void CreateKey(uint32_t mc_f_id, uint32_t mc_i_id, char *out) {
    assign_as_bigendian(mc_f_id, &out[0]);
    assign_as_bigendian(mc_i_id, &out[4]);
  }
};

struct ItemManufacturingMaster {
  alignas(CACHE_LINE_SIZE)
  std::uint32_t im_factory_id;
  std::uint32_t im_product_id;
  double im_quantity;

  static void CreateKey(uint32_t im_factory_id, uint32_t im_product_id, char *out) {
    assign_as_bigendian(im_factory_id, &out[0]);
    assign_as_bigendian(im_product_id, &out[4]);
  }
};

struct ProductCostMaster {
  alignas(CACHE_LINE_SIZE)
  std::uint32_t pc_factory_id;
  std::uint32_t pc_product_id;
  double pc_cost;
  // The product left ItemManufacturingMaster. L1 keeps rewriting the row, and S2 skips it.
  bool pc_obsolete;

  static void CreateKey(uint32_t pc_factory_id, uint32_t pc_product_id, char *out) {
    assign_as_bigendian(pc_factory_id, &out[0]);
    assign_as_bigendian(pc_product_id, &out[4]);
  }
};

struct JournalVoucher {
  alignas(CACHE_LINE_SIZE)
  std::uint64_t jv_voucher_id;
  std::uint64_t jv_date;
  std::uint32_t jv_debit;
  std::uint32_t jv_credit;
  double jv_amount;

  static void CreateKey(uint64_t jv_voucher_id, char *out) {
    assign_as_bigendian(jv_voucher_id, &out[0]);
  }
};

// A node of a bill-of-materials tree: a product, a work-in-progress item or a
// raw material (a leaf).
class Node {
public:
  Node* parent_ = nullptr;
  std::vector<Node*> childs_;
  uint32_t i_id_;
  double quantity_;
  double total_cost_ = 0;
  double unit_cost_ = 0;

  Node() {}
  Node(uint32_t i_id) : i_id_(i_id), quantity_(1.0) {}
  Node(uint32_t i_id, double q) : i_id_(i_id), quantity_(q) {}

  void add_child(Node* child) {
    child->parent_ = this;
    childs_.push_back(child);
  }

  // The root keeps its id, which lies in the root work-in-progress range.
  void assign_id(std::atomic<uint32_t>& i_id) {
    if (parent_ != nullptr)
      i_id_ = i_id++;
    for (auto& child : childs_) {
      child->assign_id(i_id);
    }
  }

  bool is_leaf() {
    return childs_.empty();
  }

  void get_all_nodes(std::vector<Node*>& nodes) {
    collect_nodes(nodes);
  }

  void collect_nodes(std::vector<Node*>& nodes) {
    nodes.push_back(this);
    for (auto& child : childs_) {
      child->collect_nodes(nodes);
    }
  }

  void get_all_leafs(std::vector<Node*>& nodes) {
    for (auto& child : childs_) {
      child->collect_leafs(nodes);
    }
  }

  // As in BoMB, this also collects the inner nodes below this node's children.
  void collect_leafs(std::vector<Node*>& nodes) {
    if (this->is_leaf()) {
      nodes.push_back(this);
    }
    for (auto& child : childs_) {
      child->collect_nodes(nodes);
    }
  }

  double calculate_cost() {
    if (this->is_leaf()) {
      return unit_cost_ * quantity_;
    }
    double subtotal = 0;
    for (auto& child : childs_) {
      subtotal += child->calculate_cost();
    }
    return subtotal * quantity_;
  }
};

struct JVID {
  union {
    uint64_t obj_;
    struct {
      uint64_t thid: 8;
      uint64_t vid: 56;
    };
  };

  JVID() : obj_(0) {};
};

template <typename Tuple, typename Param>
class BombWorkload {
public:
    Param* param_;
    Xoroshiro128Plus rnd_;
    uint64_t jv_counter_ = 0;
    std::map<uint32_t,Node*> bom_cache_;

    BombWorkload() {
        rnd_.init();
    }

    static std::chrono::nanoseconds get_request_interval_nano() {
      return std::chrono::nanoseconds(1000*1000*1000/FLAGS_bomb_mixed_short_rate);
    }

    static std::chrono::microseconds get_request_interval_micro() {
      return std::chrono::microseconds(60*1000*1000/FLAGS_bomb_mixed_short_rate);
    }

    class TxArgs {
    public:
      uint32_t f_id;
      uint32_t p_id;
      uint32_t i_id;
      uint32_t m_id;
      std::set<uint32_t> i_id_set;

      void generate(BombWorkload* w, TxExecutor& tx, TxType type) {
        switch (type) {
          case TxType::IssueJournalVoucher:
          case TxType::UpdateProductCostMaster:
            f_id = w->rnd_.random_int(1, FLAGS_bomb_factory_size);
            break;
          case TxType::UpdateMaterialCostMaster:
            f_id = w->rnd_.random_int(1, FLAGS_bomb_factory_size);
            while (i_id_set.size() < FLAGS_bomb_mc_update_size) {
              uint32_t m_id = w->rnd_.random_int(
                                get_i_id_material_start(),
                                get_i_id_material_start() + FLAGS_bomb_material_size - 1);
              i_id_set.emplace(m_id);
            }
            break;
          case TxType::AddNewProduct:
            f_id = w->rnd_.random_int(1, FLAGS_bomb_factory_size);
            p_id = ItemIdCounter.fetch_add(1) + 1;
            while (i_id_set.size() < FLAGS_bomb_tree_num_per_product) {
              uint32_t i_id = w->rnd_.random_int(
                                get_i_id_work_start(),
                                get_i_id_work_start() + FLAGS_bomb_work_size - 1);
              i_id_set.emplace(i_id);
            }
            break;
          case TxType::ChangeRawMaterial:
            // A root work-in-progress item whose tree loses a material, and the
            // material that replaces it.
            i_id = w->rnd_.random_int(
                get_i_id_work_start(),
                get_i_id_work_start() + FLAGS_bomb_work_size);
            m_id = w->rnd_.random_int(
                get_i_id_material_start(),
                get_i_id_material_start() + FLAGS_bomb_material_size - 1);
            break;
          case TxType::ChangeProductQuantity:
            f_id = w->rnd_.random_int(1, FLAGS_bomb_factory_size);
            break;
          default:
            if (loadAcquire(tx.quit_)) break;
            ERR;
        }
      }
    };

    class Query {
    public:
      TxType type;
      TxArgs args;

      // Pops the worker's next request. A worker that has to wait leaves the
      // epoch (become_quiescent) and rejoins before it runs the request.
      std::pair<TxType,timepoint> getRequest(TxExecutor& tx) {
        int queueIndex = tx.thid_ - kL1ThreadNum;
        std::pair<TxType, timepoint> ret;
        bool waited = false;
        while (!loadAcquire(tx.quit_)) {
          try {
            ret = requestQueues[queueIndex].pop();
            break;
          } catch (std::out_of_range e) {
            if (!waited) {
              waited = true;
              tx.become_quiescent();
            }
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
          }
        }
        if (waited) tx.resume_active();
        return ret;
      }

      timepoint generate(BombWorkload* workload, TxExecutor& tx) {
        timepoint start;
        if (tx.thid_ < kL1ThreadNum) {
          type = TxType::UpdateProductCostMaster;
          start = std::chrono::high_resolution_clock::now();
        } else {
          auto ret = getRequest(tx);
          type = ret.first;
          start = ret.second;
        }
        args.generate(workload, tx, type);
        return start;
      }
    };

    static const uint32_t get_i_id_product_start() {
      return 1;
    }

    static const uint32_t get_i_id_material_start() {
      return get_i_id_product_start() + FLAGS_bomb_product_size;
    }

    static const uint32_t get_i_id_work_start() {
      return get_i_id_material_start() + FLAGS_bomb_material_size;
    }

    template<typename S>
    static inline auto select_random(Xoroshiro128Plus& r, const S &s) {
      assert(!s.empty());
      auto itr = std::begin(s);
      std::advance(itr, r.random_int(0, s.size()-1));
      return itr;
    }

    template<typename T>
    inline T select_random(const std::vector<T> &v) {
      assert(!v.empty());
      auto itr = std::begin(v);
      std::advance(itr, rnd_.random_int(0, v.size()-1));
      return *itr;
    }

    // Builds the static BoM cache (all factories make the same products) and
    // closes the empty transaction reconnoiter_end() leaves open.
    void prepare(TxExecutor& tx, Param *p) {
      this->param_ = p;

      if (FLAGS_bomb_use_cache) {
        tx.reconnoiter_begin();
        std::vector<uint32_t> product_ids;
        BombWorkload<Tuple,Param>::select_im_by_factory(tx, 1, product_ids);
        for (auto& p_id : product_ids) {
          Node* root = BombWorkload<Tuple,Param>::build_bom_tree(tx, p_id);
          if (root == nullptr) ERR;
          bom_cache_.emplace(p_id, root);
        }
        tx.reconnoiter_end();
        dump(tx.thid_, "[INFO] BoM cache creation done");
      }
      tx.abort();
    }

    Status update_product_cost_master(TxExecutor& tx, uint32_t f_id, uint32_t p_id, double cost,
                                      bool obsolete = false) {
      SimpleKey<8> key;
      ProductCostMaster::CreateKey(f_id, p_id, key.ptr());
      HeapObject obj;
      obj.allocate<ProductCostMaster>();
      ProductCostMaster& pc = obj.ref();
      pc.pc_factory_id = f_id;
      pc.pc_product_id = p_id;
      pc.pc_cost = cost;
      pc.pc_obsolete = obsolete;
      Status status = tx.write(Storage::ProductCostMaster, key.view(), TupleBody(key.view(), std::move(obj)));
      return status;
    }

    // Reads the BoM tree below item `id`, one scan per node. Returns nullptr
    // if the transaction aborted (the partial tree is not freed).
    Node* build_bom_tree(TxExecutor& tx, uint32_t id) {
      SimpleKey<8> low, up;
      std::vector<TupleBody*> result;
      Node* root = new Node(id);
      std::vector<Node*> next;
      next.push_back(root);
      while (next.size() != 0) {
        auto node = next.back();
        next.pop_back();
        ItemConstructionMaster::CreateKey(node->i_id_, 0, low.ptr());
        ItemConstructionMaster::CreateKey(node->i_id_ + 1, 0, up.ptr());
        tx.scan(Storage::ItemConstructionMaster, low.view(), false, up.view(), true, result);
        if (tx.status_ == TransactionStatus::aborted) return nullptr;
        for (auto& tuple : result) {
          const ItemConstructionMaster& ic = tuple->get_value().cast_to<ItemConstructionMaster>();
          Node* n = new Node(ic.ic_i_id, ic.ic_material_quantity);
          node->add_child(n);
          next.push_back(n);
        }
      }
      return root;
    }

    void insert_journal_voucher(TxExecutor& tx, uint32_t debit, uint32_t credit, double amount) {
      JVID jv_id;
      jv_id.thid = tx.thid_;
      jv_id.vid = jv_counter_++;
      SimpleKey<8> key;
      JournalVoucher::CreateKey(jv_id.obj_, key.ptr());
      HeapObject obj;
      obj.allocate<JournalVoucher>();
      JournalVoucher& jv_tuple = obj.ref();
      jv_tuple.jv_voucher_id = jv_id.obj_;
      jv_tuple.jv_date = 20220101235959;
      jv_tuple.jv_debit = debit;
      jv_tuple.jv_credit = credit;
      jv_tuple.jv_amount = amount;
      tx.insert(Storage::JournalVoucher, key.view(), TupleBody(key.view(), std::move(obj)));
    }

    // S2: read a factory's product costs and insert one voucher per product.
    void run_issue_journal_voucher(TxExecutor& tx, Query& query) {
      uint32_t f_id = query.args.f_id;
      SimpleKey<8> low, up;
      std::vector<TupleBody*> result;
      ProductCostMaster::CreateKey(f_id, 0, low.ptr());
      ProductCostMaster::CreateKey(f_id+1, 0, up.ptr());
      tx.scan(Storage::ProductCostMaster, low.view(), false, up.view(), true, result);
      if (tx.status_ == TransactionStatus::aborted) return;

      uint32_t debit = 0;
      uint32_t credit = 1;
      for (auto& tuple : result) {
        const ProductCostMaster& pm = tuple->get_value().cast_to<ProductCostMaster>();
        if (pm.pc_obsolete) continue;
        double manufactured_quantity = 99.9;
        insert_journal_voucher(tx, debit, credit, manufactured_quantity*pm.pc_cost);
        if (tx.status_ == TransactionStatus::aborted) return;
      }
    }

    // S1: read-modify-write of material costs.
    void run_update_material_cost_master(TxExecutor& tx, Query& query) {
      for (auto& m_id : query.args.i_id_set) {
        SimpleKey<8> key;
        MaterialCostMaster::CreateKey(query.args.f_id, m_id, key.ptr());
        TupleBody* body;
        tx.read(Storage::MaterialCostMaster, key.view(), &body);
        MaterialCostMaster& old = body->get_value().cast_to<MaterialCostMaster>();
        HeapObject obj;
        obj.template allocate<MaterialCostMaster>();
        MaterialCostMaster& mc = obj.ref();
        mc.mc_f_id = old.mc_f_id;
        mc.mc_i_id = old.mc_i_id;
        mc.mc_stock_quantity = old.mc_stock_quantity + 1.0;
        mc.mc_stock_price = old.mc_stock_price + 1.0;
        tx.write(Storage::MaterialCostMaster, key.view(), TupleBody(key.view(), std::move(obj)));
      }
    }

    // Rewrites as obsolete every cost row of the factory whose product is not in
    // `costs`, so that one writer covers all of the factory's rows.
    bool mark_obsolete_product_cost_master(TxExecutor& tx, uint32_t f_id,
                                           const std::map<uint32_t,double>& costs) {
      SimpleKey<8> low, up;
      std::vector<TupleBody*> result;
      ProductCostMaster::CreateKey(f_id, 0, low.ptr());
      ProductCostMaster::CreateKey(f_id+1, 0, up.ptr());
      tx.scan(Storage::ProductCostMaster, low.view(), false, up.view(), true, result);
      if (tx.status_ == TransactionStatus::aborted) return false;

      std::vector<std::pair<uint32_t,double>> obsolete;
      for (auto& tuple : result) {
        const ProductCostMaster& pm = tuple->get_value().cast_to<ProductCostMaster>();
        if (costs.find(pm.pc_product_id) == costs.end()) {
          obsolete.emplace_back(pm.pc_product_id, pm.pc_cost);
        }
      }
      for (const auto& [p_id, cost] : obsolete) {
        update_product_cost_master(tx, f_id, p_id, cost, true);
        if (tx.status_ == TransactionStatus::aborted) return false;
        if (tx.quit_) {
          tx.abort();
          tx.status_ = TransactionStatus::invalid;
          return false;
        }
      }
      return true;
    }

    // L1 on a static BoM: cost every cached product from the factory's
    // material costs and write the product costs.
    void _run_update_product_cost_master_using_cache(TxExecutor& tx, Query& query) {
      uint32_t f_id = query.args.f_id;
      std::map<uint32_t,double> costs;
      for (const auto& [p_id, root] : bom_cache_) {
        std::vector<Node*> nodes;
        root->get_all_nodes(nodes);
        for (auto& node : nodes) {
          if (node->is_leaf()) {
            if (!get_material_cost(tx, f_id, node->i_id_, node->unit_cost_))
              return;
          }
        }
        costs.emplace(p_id, root->calculate_cost());
      }
      for (const auto& [p_id, cost] : costs) {
        update_product_cost_master(tx, f_id, p_id, cost);
        if (tx.status_ == TransactionStatus::aborted) return;
        if (tx.quit_) {
          tx.abort();
          tx.status_ = TransactionStatus::invalid;
          return;
        }
      }
      if (FLAGS_bomb_mark_obsolete) mark_obsolete_product_cost_master(tx, f_id, costs);
    }

    // L1 on a dynamic BoM: the same, but the products and their BoM trees are
    // read from the tables inside the transaction.
    void _run_update_product_cost_master(TxExecutor& tx, Query& query) {
      uint32_t f_id = query.args.f_id;
      std::vector<uint32_t> product_ids;
      Status stat = select_im_by_factory(tx, f_id, product_ids);
      if (stat != Status::OK) return;
      if (product_ids.size() == 0) {
        dump(tx.thid_, "ERROR: No target records.");
        ERR;
      }

      std::map<uint32_t,double> costs;
      for (auto& p_id : product_ids) {
        Node* root = build_bom_tree(tx, p_id);
        if (root == nullptr) return;
        std::vector<Node*> nodes;
        root->get_all_nodes(nodes);
        for (auto& node : nodes) {
          if (node->is_leaf()) {
            if (!get_material_cost(tx, f_id, node->i_id_, node->unit_cost_))
              return;
          }
        }
        costs.emplace(p_id, root->calculate_cost());
      }
      for (const auto& [p_id, cost] : costs) {
        Status status = update_product_cost_master(tx, f_id, p_id, cost);
        if (status == Status::WARN_NOT_FOUND) {
          status = insert_product_cost_master(tx, f_id, p_id, 0.0);
          if (stat != Status::OK) {
            std::stringstream ss;
            ss << "ERROR: insert product cost master fails " << f_id << " " << p_id;
            dump(tx.thid_, ss.str());
            ERR;
          }
        }
        if (tx.status_ == TransactionStatus::aborted) return;
        if (tx.quit_) {
          tx.abort();
          tx.status_ = TransactionStatus::invalid;
          return;
        }
      }
      if (FLAGS_bomb_mark_obsolete) mark_obsolete_product_cost_master(tx, f_id, costs);
    }

    void run_update_product_cost_master(TxExecutor& tx, Query& query) {
      if (FLAGS_bomb_use_cache) {
        _run_update_product_cost_master_using_cache(tx, query);
      } else {
        _run_update_product_cost_master(tx, query);
      }
    }

    Status insert_item_construction_master(TxExecutor& tx,
                                           uint32_t parent_id, uint32_t id, double material_quantity) {
      SimpleKey<8> key;
      ItemConstructionMaster::CreateKey(parent_id, id, key.ptr());
      HeapObject obj;
      obj.allocate<ItemConstructionMaster>();
      ItemConstructionMaster& ic_tuple = obj.ref();
      ic_tuple.ic_parent_i_id = parent_id;
      ic_tuple.ic_i_id = id;
      ic_tuple.ic_material_quantity = material_quantity;
      return tx.insert(Storage::ItemConstructionMaster, key.view(), TupleBody(key.view(), std::move(obj)));
    }

    Status insert_item_manufacturing_master(TxExecutor& tx,
                                            uint32_t factory_id, uint32_t product_id, double quantity) {
      SimpleKey<8> key;
      ItemManufacturingMaster::CreateKey(factory_id, product_id, key.ptr());
      HeapObject obj;
      obj.allocate<ItemManufacturingMaster>();
      ItemManufacturingMaster& im_tuple = obj.ref();
      im_tuple.im_factory_id = factory_id;
      im_tuple.im_product_id = product_id;
      im_tuple.im_quantity = quantity;
      return tx.insert(Storage::ItemManufacturingMaster, key.view(), TupleBody(key.view(), std::move(obj)));
    }

    Status insert_product_cost_master(TxExecutor& tx, uint32_t f_id, uint32_t p_id, double cost) {
      SimpleKey<8> key;
      ProductCostMaster::CreateKey(f_id, p_id, key.ptr());
      HeapObject obj;
      obj.allocate<ProductCostMaster>();
      ProductCostMaster& p_tuple = obj.ref();
      p_tuple.pc_factory_id = f_id;
      p_tuple.pc_product_id = p_id;
      p_tuple.pc_cost = cost;
      p_tuple.pc_obsolete = false;
      return tx.insert(Storage::ProductCostMaster, key.view(), TupleBody(key.view(), std::move(obj)));
    }

    Status delete_item_construction_master(TxExecutor& tx, uint32_t parent_id, uint32_t material_id) {
      SimpleKey<8> key;
      ItemConstructionMaster::CreateKey(parent_id, material_id, key.ptr());
      return tx.delete_record(Storage::ItemConstructionMaster, key.view());
    }

    Status delete_item_manufacturing_master(TxExecutor& tx, uint32_t factory_id, uint32_t product_id) {
      SimpleKey<8> key;
      ItemManufacturingMaster::CreateKey(factory_id, product_id, key.ptr());
      return tx.delete_record(Storage::ItemManufacturingMaster, key.view());
    }

    Status update_item_manufacturing_master(TxExecutor& tx,
                                            uint32_t factory_id, uint32_t product_id, double quantity) {
      SimpleKey<8> key;
      ItemManufacturingMaster::CreateKey(factory_id, product_id, key.ptr());
      HeapObject obj;
      obj.allocate<ItemManufacturingMaster>();
      ItemManufacturingMaster& im_tuple = obj.ref();
      im_tuple.im_factory_id = factory_id;
      im_tuple.im_product_id = product_id;
      im_tuple.im_quantity = quantity;
      return tx.write(Storage::ItemManufacturingMaster, key.view(), TupleBody(key.view(), std::move(obj)));
    }

    Status run_add_new_prodcut(TxExecutor& tx, Query& query) {
      Status stat;
      uint32_t f_id = query.args.f_id;
      uint32_t p_id = query.args.p_id;
      for (auto& i_id : query.args.i_id_set) {
        stat = insert_item_construction_master(tx, p_id, i_id, 1.0);
        if (stat != Status::OK) {
          std::stringstream ss;
          ss << "WARN: insert bom table fails " << f_id << " " << i_id;
          dump(tx.thid_, ss.str());
          return stat;
        }
      }

      stat = insert_item_manufacturing_master(tx, f_id, p_id, 10.0);
      if (stat != Status::OK) {
        std::stringstream ss;
        ss << "WARN: insert product table fails " << f_id << " " << p_id;
        dump(tx.thid_, ss.str());
      }

      return stat;
    }

    // The target is chosen outside the transaction, as if from a client cache.
    Status run_delete_product(TxExecutor& tx, Query& query) {
      std::vector<uint32_t> product_ids;
      tx.reconnoiter_begin();
      Status stat = select_im_by_factory(tx, query.args.f_id, product_ids);
      tx.reconnoiter_end();
      if (stat != Status::OK) return stat;

      uint32_t product_id = select_random(product_ids);
      stat = delete_item_manufacturing_master(tx, query.args.f_id, product_id);
      if (stat != Status::OK) {
        dump(tx.thid_, "delete product fail");
      }
      return stat;
    }

    // S3: replace one of a factory's products by a new one.
    void run_change_product(TxExecutor& tx, Query& query) {
      Status stat = run_delete_product(tx, query);
      if (stat != Status::OK) {
        dump(tx.thid_, "delete product fail");
        return;
      } else {
        run_add_new_prodcut(tx, query);
      }
    }

    // S4: replace one material of a work-in-progress tree.
    void run_change_raw_material(TxExecutor& tx, Query& query) {
      std::vector<std::pair<uint32_t,uint32_t>> material_ids;
      tx.reconnoiter_begin();
      auto ret = select_materials_by_item_id(tx, query.args.i_id, material_ids);
      tx.reconnoiter_end();
      if (ret != Status::OK) return;

      auto pair = select_random(material_ids);
      auto parent_id = pair.first;
      auto material_id = pair.second;
      Status stat = delete_item_construction_master(tx, parent_id, material_id);
      if (stat != Status::OK) return;
      insert_item_construction_master(tx, parent_id, query.args.m_id, 10.0);
    }

    // S5: update the quantity of one of a factory's products.
    void run_change_product_quantity(TxExecutor& tx, Query& query) {
      std::vector<uint32_t> product_ids;
      tx.reconnoiter_begin();
      Status stat = select_im_by_factory(tx, query.args.f_id, product_ids);
      tx.reconnoiter_end();
      if (stat != Status::OK) return;
      if (product_ids.size() == 0) {
        dump(tx.thid_, "ERROR: No target records.");
        ERR;
      }

      uint32_t product_id = select_random(product_ids);
      update_item_manufacturing_master(tx, query.args.f_id, product_id, 10.0);
    }

    bool get_material_cost(TxExecutor& tx, uint32_t f_id, uint32_t m_id, double& cost) {
      SimpleKey<8> key;
      MaterialCostMaster::CreateKey(f_id, m_id, key.ptr());
      TupleBody* body;
      tx.read(Storage::MaterialCostMaster, key.view(), &body);
      if (tx.status_ == TransactionStatus::aborted) return false;
      MaterialCostMaster& mc = body->get_value().cast_to<MaterialCostMaster>();
      cost = mc.mc_stock_price / mc.mc_stock_quantity;
      return true;
    }

    // (parent, item) pairs below work-in-progress item `i_id`.
    Status select_materials_by_item_id(TxExecutor& tx, uint32_t i_id,
        std::vector<std::pair<uint32_t,uint32_t>>& material_ids) {
      Node* parent = build_bom_tree(tx, i_id);
      if (parent == nullptr) ERR;

      std::vector<Node*> nodes;
      parent->get_all_leafs(nodes);

      if (nodes.size() == 0) ERR;
      for (auto& node : nodes) {
        material_ids.emplace_back(node->parent_->i_id_, node->i_id_);
      }
      return Status::OK;
    }

    Status select_im_by_factory(TxExecutor& tx, uint32_t factory_id, std::vector<uint32_t>& product_ids) {
      SimpleKey<8> low, up;
      std::vector<TupleBody*> result;
      ItemManufacturingMaster::CreateKey(factory_id, 0, low.ptr());
      ItemManufacturingMaster::CreateKey(factory_id+1, 0, up.ptr());
      Status stat = tx.scan(Storage::ItemManufacturingMaster, low.view(), false, up.view(), true, result);
      if (tx.status_ == TransactionStatus::aborted) return stat;
      for (auto& tuple : result) {
        const ItemManufacturingMaster& im = tuple->get_value().cast_to<ItemManufacturingMaster>();
        product_ids.emplace_back(im.im_product_id);
      }
      return stat;
    }

    // Runs one request to commit, retrying it after every abort.
    template <typename TxExecutor, typename TransactionStatus>
    void run(TxExecutor& tx) {
        Query query;
        auto start = query.generate(this, tx);
        const auto eval_svc_start = eval::g_on
            ? std::chrono::high_resolution_clock::now() : start;

RETRY:
        if (tx.isLeader()) {
            tx.leaderWork();
        }

        if (loadAcquire(tx.quit_)) return;

        tx.begin();

        switch (query.type) {
          case TxType::IssueJournalVoucher:
            run_issue_journal_voucher(tx, query);
            break;
          case TxType::UpdateMaterialCostMaster:
            run_update_material_cost_master(tx, query);
            break;
          case TxType::UpdateProductCostMaster:
            run_update_product_cost_master(tx, query);
            break;
          case TxType::AddNewProduct:
            run_change_product(tx, query);
            break;
          case TxType::ChangeRawMaterial:
            run_change_raw_material(tx, query);
            break;
          case TxType::ChangeProductQuantity:
            run_change_product_quantity(tx, query);
            break;
          default:
            ERR;
            break;
        }

        if (tx.status_ == TransactionStatus::aborted) {
            tx.abort();
            tx.result_->local_abort_counts_++;
            tx.result_->local_abort_counts_per_tx_[get_tx_type(query.type)]++;
            eval::on_abort(tx.thid_, get_tx_type(query.type), false);
            goto RETRY;
        }

        if (!tx.commit()) {
            tx.abort();
            if (tx.status_ == TransactionStatus::invalid) return;
            tx.result_->local_abort_counts_++;
            tx.result_->local_abort_counts_per_tx_[get_tx_type(query.type)]++;
            eval::on_abort(tx.thid_, get_tx_type(query.type), true);
            goto RETRY;
        }

        if (loadAcquire(tx.quit_)) return;
        tx.result_->local_commit_counts_++;
        tx.result_->local_commit_counts_per_tx_[get_tx_type(query.type)]++;

        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        tx.result_->local_latency_per_tx_[get_tx_type(query.type)] =
            tx.result_->local_latency_per_tx_[get_tx_type(query.type)] + elapsed.count();
        if (eval::g_on) {
            eval::on_commit(tx.thid_, get_tx_type(query.type), elapsed.count(),
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end - eval_svc_start).count());
        }
        return;
    }

    static void insert_item_construction_master([[maybe_unused]] size_t thid, Param *param,
                                                uint32_t parent_id, uint32_t id,
                                                double material_quantity) {
      SimpleKey<8> key;
      ItemConstructionMaster::CreateKey(parent_id, id, key.ptr());
      HeapObject obj;
      obj.allocate<ItemConstructionMaster>();
      ItemConstructionMaster& ic_tuple = obj.ref();
      ic_tuple.ic_parent_i_id = parent_id;
      ic_tuple.ic_i_id = id;
      ic_tuple.ic_material_quantity = material_quantity;
      Tuple* tmp = new Tuple();
      tmp->init(thid, TupleBody(key.view(), std::move(obj)), param);
      Masstrees[get_storage(Storage::ItemConstructionMaster)].insert_value(key.view(), tmp);
    }

    static void insert_item_manufacturing_master([[maybe_unused]] size_t thid, Param *param,
                                                 uint32_t factory_id, uint32_t product_id,
                                                 double quantity) {
      SimpleKey<8> key;
      ItemManufacturingMaster::CreateKey(factory_id, product_id, key.ptr());
      HeapObject obj;
      obj.allocate<ItemManufacturingMaster>();
      ItemManufacturingMaster& im_tuple = obj.ref();
      im_tuple.im_factory_id = factory_id;
      im_tuple.im_product_id = product_id;
      im_tuple.im_quantity = quantity;
      Tuple* tmp = new Tuple();
      tmp->init(thid, TupleBody(key.view(), std::move(obj)), param);
      Masstrees[get_storage(Storage::ItemManufacturingMaster)].insert_value(key.view(), tmp);
    }

    static void insert_material_cost_master([[maybe_unused]] size_t thid, Param *p,
                                            uint32_t f_id, uint32_t m_id, double quantity, double price) {
      SimpleKey<8> key;
      MaterialCostMaster::CreateKey(f_id, m_id, key.ptr());
      HeapObject obj;
      obj.allocate<MaterialCostMaster>();
      MaterialCostMaster& m_tuple = obj.ref();
      m_tuple.mc_f_id = f_id;
      m_tuple.mc_i_id = m_id;
      m_tuple.mc_stock_quantity = quantity;
      m_tuple.mc_stock_price = price;
      Tuple* tmp = new Tuple();
      tmp->init(thid, TupleBody(key.view(), std::move(obj)), p);
      Masstrees[get_storage(Storage::MaterialCostMaster)].insert_value(key.view(), tmp);
    }

    static void insert_product_cost_master([[maybe_unused]] size_t thid, Param *p,
                                         uint32_t f_id, uint32_t p_id, double cost) {
      SimpleKey<8> key;
      ProductCostMaster::CreateKey(f_id, p_id, key.ptr());
      HeapObject obj;
      obj.allocate<ProductCostMaster>();
      ProductCostMaster& p_tuple = obj.ref();
      p_tuple.pc_factory_id = f_id;
      p_tuple.pc_product_id = p_id;
      p_tuple.pc_cost = cost;
      p_tuple.pc_obsolete = false;
      Tuple* tmp = new Tuple();
      tmp->init(thid, TupleBody(key.view(), std::move(obj)), p);
      Masstrees[get_storage(Storage::ProductCostMaster)].insert_value(key.view(), tmp);
    }

    // Loads root work-in-progress items start..end: a random tree of
    // work-in-progress nodes with raw materials below its leaves.
    static void load_work_in_progress([[maybe_unused]] size_t thid, Param* param,
                                      std::atomic<uint32_t>& i_id, uint64_t start, uint64_t end) {
      MasstreeWrapper<Tuple>::thread_init(thid);
      Xoroshiro128Plus rand;
      rand.init();
      for (auto i = start; i <= end; ++i) {
        auto tree_size = FLAGS_bomb_base_tree_size;

        Node* root = new Node();
        std::vector<Node*> nodes;
        nodes.push_back(root);

        while (nodes.size() < tree_size) {
          assert(!nodes.empty());
          Node* node = new Node();
          Node* parent = nodes.at(rand.random_int(0, nodes.size()-1));
          parent->add_child(node);
          nodes.push_back(node);
        }

        root->i_id_ = get_i_id_work_start() + i;
        root->assign_id(i_id);

        for (auto& node : nodes) {
          if (!node->is_leaf()) continue;
          auto material_size = FLAGS_bomb_material_per_wip;
          std::set<uint32_t> s;
          while (s.size() < material_size)
            s.emplace(rand.random_int(get_i_id_material_start(),
                                      get_i_id_material_start() + FLAGS_bomb_material_size - 1));
          for (auto& i_id_material : s)
            node->add_child(new Node(i_id_material));
        }

        nodes.clear();
        root->get_all_nodes(nodes);
        for (auto& node : nodes) {
          if (node->parent_ == nullptr) continue;
          insert_item_construction_master(thid, param, node->parent_->i_id_, node->i_id_, 1.0);
        }
      }
    }

    static void load_item_construction_master_product([[maybe_unused]] size_t thid, Param *param,
                                                      uint32_t p_id, std::set<uint32_t>& wip_ids) {
      for (auto& id : wip_ids) {
          insert_item_construction_master(thid, param, p_id, id, 1.0);
      }
    }

    // Every factory manufactures the same randomly chosen products.
    static void load_item_manufacturing_master([[maybe_unused]] size_t thid, Param *param,
                                               Xoroshiro128Plus& rand,
                                               std::set<uint32_t>& product_ids,
                                               std::vector<std::tuple<uint32_t,uint32_t>>& pm_keys) {
      for (uint32_t i = 0; i < FLAGS_bomb_base_product_size_per_factory; i++) {
        auto itr = select_random(rand, product_ids);
        for (uint32_t f_id = 1; f_id <= FLAGS_bomb_factory_size; f_id++) {
          insert_item_manufacturing_master(0, param, f_id, *itr, 1.0);
          pm_keys.emplace_back(f_id, *itr);
        }
        product_ids.erase(itr);
      }
    }

    static void load_material_cost_master([[maybe_unused]] size_t thid, Param *param) {
      for (uint32_t f_id = 1; f_id <= FLAGS_bomb_factory_size; f_id++) {
        for (uint32_t m_id = get_i_id_material_start();
             m_id < get_i_id_material_start() + FLAGS_bomb_material_size; m_id++) {
          insert_material_cost_master(0, param, f_id, m_id, 1.0, 1.0);
        }
      }
    }

    static void load_product_cost_master([[maybe_unused]] size_t thid, Param *param,
                                          std::vector<std::tuple<uint32_t,uint32_t>>& pm_keys) {
      for (auto& [f_id, p_id] : pm_keys) {
        insert_product_cost_master(0, param, f_id, p_id, 0.0);
      }
    }

    static uint32_t getTableNum() {
      return (uint32_t)Storage::Size;
    }

    static void makeDB(Param* param) {
      Xoroshiro128Plus rand;
      rand.init();
      size_t maxthread = 64;
      std::vector<std::thread> thv;
      auto product_start = get_i_id_product_start();
      auto root_wip_start = get_i_id_work_start();
      auto non_root_wip_start = get_i_id_work_start() + FLAGS_bomb_work_size;
      ItemIdCounter.store(non_root_wip_start);

      set_tx_name(TxType::UpdateMaterialCostMaster, "UpdateMaterialCostMaster");
      set_tx_name(TxType::UpdateProductCostMaster, "UpdateProductCostMaster");
      set_tx_name(TxType::IssueJournalVoucher, "IssueJournalVoucher");
      set_tx_name(TxType::AddNewProduct, "AddNewProduct");
      set_tx_name(TxType::ChangeRawMaterial, "ChangeRawMaterial");
      set_tx_name(TxType::ChangeProductQuantity, "ChangeProductQuantity");

      std::cout << "load item construction master for work in progress" << std::endl;
      for (size_t i = 0; i < maxthread; i++) {
        thv.emplace_back(load_work_in_progress, i, param, std::ref(ItemIdCounter),
                        i * (FLAGS_bomb_work_size / maxthread),
                        (i + 1) * ((FLAGS_bomb_work_size + maxthread - 1) / maxthread) - 1);
      }
      for (auto &th : thv) th.join();

      std::cout << "load item construction master for finished products" << std::endl;
      std::set<uint32_t> product_id_set;
      for (uint32_t p_id = product_start;
           p_id < product_start + FLAGS_bomb_product_size; p_id++) {
        auto tree_size = FLAGS_bomb_tree_num_per_product;
        std::set<uint32_t> s;
        while (s.size() < tree_size)
          s.emplace(rand.random_int(root_wip_start,
                                    root_wip_start + FLAGS_bomb_work_size - 1));
        load_item_construction_master_product(0, param, p_id, s);
        product_id_set.emplace(p_id);
      }

      std::cout << "load item manufacturing master " << std::endl;
      std::vector<std::tuple<uint32_t,uint32_t>> pm_keys;
      load_item_manufacturing_master(0, param, rand, product_id_set, pm_keys);

      std::cout << "load material cost master " << std::endl;
      load_material_cost_master(0, param);

      std::cout << "load product cost master " << std::endl;
      load_product_cost_master(0, param, pm_keys);

      std::cout << "loading done" << std::endl;
    }

    static TxType decideType(Xoroshiro128Plus& r, uint64_t* thresholds) {
      uint64_t x = r.random_int(1, 100);
      if (x > thresholds[0]) return TxType::UpdateMaterialCostMaster;
      if (x > thresholds[1]) return TxType::IssueJournalVoucher;
      if (x > thresholds[2]) return TxType::AddNewProduct;
      if (x > thresholds[3]) return TxType::ChangeRawMaterial;
      return TxType::ChangeProductQuantity;
    }

    // Fills the short workers' queues round-robin: one batch of requests per
    // queue, then one request interval of sleep.
    static void request_dispatcher(size_t thid, char &ready, const bool &start, const bool &quit) {
      int numQueues = TotalThreadNum - kL1ThreadNum;
      requestQueues = new ConcurrentQueue<std::pair<TxType, timepoint>>[numQueues];

      uint64_t thresholds[4];
      int32_t perc_s5 = 100 - (FLAGS_bomb_perc_s1 + FLAGS_bomb_perc_s2 + FLAGS_bomb_perc_s3 + FLAGS_bomb_perc_s4);
      if (perc_s5 < 0) {
        std::cout << "Specify short transaction percentage to make the total 100" << std::endl;
        ERR;
      }
      thresholds[3] = perc_s5;
      thresholds[2] = thresholds[3] + FLAGS_bomb_perc_s4;
      thresholds[1] = thresholds[2] + FLAGS_bomb_perc_s3;
      thresholds[0] = thresholds[1] + FLAGS_bomb_perc_s2;

      Xoroshiro128Plus r;
      storeRelease(ready, 1);
      while (!loadAcquire(start)) _mm_pause();

      while (!loadAcquire(quit)) {
        for (int i = 0; i < numQueues; i++) {
          auto start = std::chrono::high_resolution_clock::now();
          for (int j = 0; j < FLAGS_bomb_req_batch_size; j++) {
            requestQueues[i].push(make_pair(decideType(r, thresholds), start));
          }
          eval::on_dispatch(FLAGS_bomb_req_batch_size);
          if (FLAGS_bomb_mixed_short_rate_tps) {
            std::this_thread::sleep_for(get_request_interval_nano());
          } else {
            std::this_thread::sleep_for(get_request_interval_micro());
          }
        }
      }
      sleepMs(1000);
      delete[] requestQueues;
    }
};
