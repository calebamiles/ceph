#include <errno.h>

#include <string>
#include <map>

#include "common/ceph_json.h"

#include "common/errno.h"
#include "rgw_rados.h"

#include "include/types.h"
#include "rgw_zone.h"
#include "rgw_string.h"

#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static int read_input(const string& infile, bufferlist& bl)
{
  int fd = 0;
  if (infile.size()) {
    fd = open(infile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = -errno;
      dout(0) << "error reading input file " << infile << dendl;
      return err;
    }
  }

#define READ_CHUNK 8196
  int r;

  do {
    char buf[READ_CHUNK];

    r = read(fd, buf, READ_CHUNK);
    if (r < 0) {
      int err = -errno;
      dout(0) << "error while reading input" << dendl;
      return err;
    }
    bl.append(buf, r);
  } while (r > 0);

  if (infile.size()) {
    close(fd);
  }

  return 0;
}

template <class T>
static int read_decode_json(const string& infile, T& t)
{
  bufferlist bl;
  int ret = read_input(infile, bl);
  if (ret < 0) {
    dout(0) << "ERROR: failed to read input: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  JSONParser p;
  ret = p.parse(bl.c_str(), bl.length());
  if (ret < 0) {
    cout << "failed to parse JSON" << std::endl;
    return ret;
  }

  try {
    t.decode_json(&p);
  } catch (JSONDecoder::err& e) {
    dout(0) << "failed to decode JSON input: " << e.message << dendl;
    return -EINVAL;
  }
  return 0;
}

int RGWZoneAdminOp::zone_info(RGWRados *store,RGWFormatterFlusher& flusher)
{
  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  store->zone.dump(formatter);
  flusher.flush();

  return 0;
}

int RGWZoneAdminOp::zone_set(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWZoneParams zone;
  zone.init_default();

  std::string infile = op_state.get_infile();

  int ret = read_decode_json(infile, zone);
  if (ret < 0) {
    return ret;
  }

  ret = zone.store_info(g_ceph_context, store);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);
  zone.dump(formatter);
  flusher.flush();

  return 0;
}

int RGWZoneAdminOp::add_pools(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  std::set<std::string> pool_set = op_state.get_pool_set();
  std::vector<std::string> pool_names(pool_set.begin(), pool_set.end());
  std::vector<int> temp_codes;
  std::map<std::string, int> return_codes;

  if (pool_names.empty())
    return -EINVAL;

  bool create_pool = op_state.will_create_pools();

  if (create_pool)
    store->create_pools(pool_names, temp_codes);

  for (unsigned i = 0; i < pool_names.size(); ++i) {
    if (create_pool && temp_codes[i] < 0)
      return_codes[pool_names[i]] = temp_codes[i];
    else
      return_codes[pool_names[i]] = store->add_bucket_placement(pool_names[i]);
  }

  if (!op_state.is_silent()) {
    Formatter *formatter = flusher.get_formatter();
    flusher.start(0);

    std::map<std::string, int>::iterator return_iter = return_codes.begin();
    formatter->open_array_section("results");

    for (; return_iter != return_codes.end(); ++return_iter) {
      std::string pool_name = return_iter->first;
      formatter->dump_int(pool_name.c_str(), return_iter->second);
     }

    flusher.flush();
  }

  return 0;
}

int RGWZoneAdminOp::remove_pools(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  std::map<std::string, int> return_codes;
  std::set<std::string> pool_names = op_state.get_pool_set();
  if (pool_names.empty())
    return -EINVAL;

  std::set<std::string>::iterator  name_iter = pool_names.begin();
  for (; name_iter != pool_names.end(); ++name_iter) {
    std::string pool_name = *name_iter;
    return_codes[pool_name] = store->remove_bucket_placement(pool_name);
  }

  if (!op_state.is_silent()) {
    Formatter *formatter = flusher.get_formatter();
    flusher.start(0);

    std::map<std::string, int>::iterator return_iter = return_codes.begin();
    formatter->open_array_section("results");

    for (; return_iter != return_codes.end(); ++return_iter) {
      std::string pool_name = return_iter->first;
      formatter->dump_int(pool_name.c_str(), return_iter->second);
     }

    formatter->close_section();
    flusher.flush();
  }

  return 0;
}

int RGWZoneAdminOp::list_pools(RGWRados *store, RGWFormatterFlusher& flusher)
{
  set<string> pools;
  int ret = store->list_placement_set(pools);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_array_section("pools");
  set<string>::iterator siter;
  for (siter = pools.begin(); siter != pools.end(); ++siter) {
    formatter->open_object_section("pool");
    formatter->dump_string("name",  *siter);
    formatter->close_section();
  }

  formatter->close_section();
  flusher.flush();

  return 0;
}

int RGWZoneAdminOp::list_garbage(RGWRados *store, RGWFormatterFlusher& flusher)
{
  int ret;
  int index = 0;
  string marker;
  bool truncated;

  list<cls_rgw_gc_obj_info> result;
  list<cls_rgw_gc_obj_info>::iterator iter;

  do {
    list<cls_rgw_gc_obj_info> temp_result;
    ret = store->list_gc_objs(&index, marker, 1000, temp_result, &truncated);
    if (ret < 0) {
      return ret;
    }

    list<cls_rgw_gc_obj_info>::iterator  result_end = result.end();
    result.insert(result_end, temp_result.begin(), temp_result.end());
  } while (truncated);


  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_array_section("entries");

  for (iter = result.begin(); iter != result.end(); ++iter) {
    cls_rgw_gc_obj_info& info = *iter;
    formatter->open_object_section("chain_info");
    formatter->dump_string("tag", info.tag);
    formatter->dump_stream("time") << info.time;
    formatter->open_array_section("objs");
    list<cls_rgw_obj>::iterator liter;
    cls_rgw_obj_chain& chain = info.chain;

    for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
      cls_rgw_obj& obj = *liter;
      formatter->dump_string("pool", obj.pool);
      formatter->dump_string("oid", obj.oid);
      formatter->dump_string("key", obj.key);
    }

    formatter->close_section(); // objs
    formatter->close_section(); // obj_chain

    if (index % 1000) // flush every 1000 entries
      flusher.flush();

    index--;
  }

  formatter->close_section();
  flusher.flush();

  return 0;
}

int RGWZoneAdminOp::process_garbage(RGWRados *store)
{
  return store->process_gc();
}

int RGWZoneAdminOp::show_logs(RGWRados *store, RGWZoneAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  std::list<std::string> logs;
  std::list<rgw_log_entry> entries;
  RGWAccessHandle h;

  std::string date = op_state.get_date();

  uint64_t agg_time = 0;
  uint64_t agg_bytes_sent = 0;
  uint64_t agg_bytes_received = 0;
  uint64_t total_entries = 0;

  bool show_log_entries = op_state.will_show_log_entries();
  bool skip_zero_entries = op_state.will_skip_zero_entries();
  bool show_log_sum = op_state.will_show_log_sum();
  bool list_logs = op_state.will_list_logs();

  if (list_logs) {
    int r = store->log_list_init(date, &h);
    if (r < 0 && r != -ENOENT)
      return r;

    while (r != -ENOENT) {
      string name;
      r = store->log_list_next(h, &name);
      if (r < 0 && r != -ENOENT)
	return r;

      if (!name.empty())
        logs.push_back(name);
    }
  } else {
    std::string log_object = op_state.get_log_object();
    int r = store->log_show_init(log_object, &h);
    if (r < 0)
      return -r;

    struct rgw_log_entry entry;

    // peek at first entry to get bucket metadata
    r = store->log_show_next(h, &entry);
    if (r < 0)
      return -r;

    entries.push_back(entry);

    do {
      uint64_t total_time =  entry.total_time.sec() * 1000000LL * entry.total_time.usec();

      agg_time += total_time;
      agg_bytes_sent += entry.bytes_sent;
      agg_bytes_received += entry.bytes_received;
      total_entries++;

      r = store->log_show_next(h, &entry);

      // entry will have been updated by log_show_next
      entries.push_back(entry);
    } while (r > 0);

    if (r < 0)
      return r;
  }

  // either list the log objects or dump a log entry
  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  if (list_logs || !logs.empty()) {
    std::list<string>::iterator logs_iter = logs.begin();
    formatter->open_array_section("logs");

    for (; logs_iter != logs.end(); ++logs_iter) {
      formatter->dump_string("object", *logs_iter);
    }

    formatter->close_section();
    flusher.flush();
  } else if (!entries.empty()) {
    std::list<rgw_log_entry>::iterator entries_iter = entries.begin();
    formatter->open_object_section("log");

    // peek at first entry to get bucket metadata
    formatter->dump_string("bucket_id", entries_iter->bucket_id);
    formatter->dump_string("bucket_owner", entries_iter->bucket_owner);
    formatter->dump_string("bucket", entries_iter->bucket);

    if (show_log_entries) {
      formatter->open_array_section("log_entries");

     while (entries_iter != entries.end()) {
       entries_iter++;
       if (entries_iter == entries.end())
         break;

      rgw_log_entry entry = *entries_iter;

       if (skip_zero_entries && entry.bytes_sent == 0 &&
               entry.bytes_received == 0) {
          continue;
       }

        rgw_format_ops_log_entry(entry, formatter);
        flusher.flush();

      }

      formatter->close_section();
    }

    if (show_log_sum) {
      formatter->open_object_section("log_sum");
      formatter->dump_int("bytes_sent", agg_bytes_sent);
      formatter->dump_int("bytes_received", agg_bytes_received);
      formatter->dump_int("total_time", agg_time);
      formatter->dump_int("total_entries", total_entries);
      formatter->close_section();
    }

    formatter->close_section();
    flusher.flush();
  }

  return 0;
}

int RGWZoneAdminOp::remove_log(RGWRados *store, RGWZoneAdminOpState& op_state)
{
  std::string oid = op_state.get_log_object();
  if (oid.empty())
    return -EINVAL;

  return store->log_remove(oid);
}

