#ifndef CEPH_RGW_ZONE_H
#define CEPH_RGW_ZONE_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "rgw_formats.h"

#include "include/str_list.h"

using namespace std;


// simple wrappers for the RESTful API

struct RGWZoneAdminOpState {
  std::string infile;
  std::string date;
  std::string object;
  std::string log_object;
  std::string bucket_name;
  std::string bucket_id;

  std::set<std::string>  pool_names;

  bool create_pool;
  bool show_log_entries;
  bool skip_zero_entries;
  bool show_log_sum;
  bool silent;

  void set_create_pools() { create_pool = true; };
  void set_show_log_entries() { show_log_entries = true; };
  void set_skip_zero_entries() { skip_zero_entries = true; };
  void set_show_log_sum() { show_log_sum = true; };
  void set_silent() { silent = true; };
  void set_verbose() { silent = false; };

  void set_infile(std::string& file) {
    if (!file.empty())
      infile = file;
  }
  void set_date(std::string& d) {
    if (!(d.empty() || date.size() != 10))
      date = d;
  }
  void set_object(std::string& o) {
    if (!o.empty())
      object = o;
  }
  void set_bucket_name(std::string& bname) {
    if (!bname.empty())
      bucket_name = bname;
  }
  void set_bucket_id(std::string& bid) {
    if (!bid.empty())
      bucket_id = bid;
  }
  void add_pool_name(std::string& name) {
    if (!name.empty())
      pool_names.insert(name);
  }
  void set_pools(std::string& names) {
    get_str_set(names, pool_names);
  }
  void clear_pool_list() { pool_names.clear(); };

  std::string get_log_object() {
    if (!object.empty()) {
      return object;
    } else if (!log_object.empty()) {
      return log_object;
    } else if (!(date.empty() || bucket_name.empty() || bucket_id.empty())) {
      log_object = date;
      log_object += "-";
      log_object += bucket_id;
      log_object += "-";
      log_object += bucket_name;

      return log_object;
    }

    return "";
  }

  std::string& get_date() { return date; };
  std::string& get_infile() { return infile; };
  std::string& get_bucket_name() { return bucket_name; };
  std::string& get_bucekt_id() { return bucket_id; };

  std::set<std::string>& get_pool_set() { return pool_names; };

  bool will_show_log_entries() { return show_log_entries; };
  bool will_skip_zero_entries() { return skip_zero_entries; };
  bool will_show_log_sum() { return show_log_sum; };
  bool will_create_pools() { return create_pool; };
  bool is_silent() { return silent; };

  bool will_list_logs() {
    return (bucket_name.empty() && bucket_id.empty() && object.empty());
  }

  RGWZoneAdminOpState() : create_pool(false), show_log_entries(false),
                          skip_zero_entries(false), show_log_sum(false),
                          silent(true) {}

};

class RGWZoneAdminOp
{
public:
  static int zone_info(RGWRados *store, RGWFormatterFlusher& flusher);

  static int zone_set(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int add_pools(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int remove_pools(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int list_pools(RGWRados *store, RGWFormatterFlusher& flusher);

  static int list_garbage(RGWRados *store, RGWFormatterFlusher& flusher);

  static int process_garbage(RGWRados *store);

  static int show_logs(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int remove_log(RGWRados *store, RGWZoneAdminOpState& op_state);

};

#endif
