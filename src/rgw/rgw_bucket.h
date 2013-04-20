#ifndef CEPH_RGW_BUCKET_H
#define CEPH_RGW_BUCKET_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "rgw_formats.h"


using namespace std;

// define as static when RGWBucket implementation compete
extern void rgw_get_buckets_obj(string& user_id, string& buckets_obj_id);


/**
 * Store a list of the user's buckets, with associated functinos.
 */
class RGWUserBuckets
{
  map<string, RGWBucketEnt> buckets;

public:
  RGWUserBuckets() {}
  void encode(bufferlist& bl) const {
    ::encode(buckets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(buckets, bl);
  }
  /**
   * Check if the user owns a bucket by the given name.
   */
  bool owns(string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    return (iter != buckets.end());
  }

  /**
   * Add a (created) bucket to the user's bucket list.
   */
  void add(RGWBucketEnt& bucket) {
    buckets[bucket.bucket.name] = bucket;
  }

  /**
   * Remove a bucket from the user's list by name.
   */
  void remove(string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    if (iter != buckets.end()) {
      buckets.erase(iter);
    }
  }

  /**
   * Get the user's buckets as a map.
   */
  map<string, RGWBucketEnt>& get_buckets() { return buckets; }

  /**
   * Cleanup data structure
   */
  void clear() { buckets.clear(); }

  size_t count() { return buckets.size(); }
};
WRITE_CLASS_ENCODER(RGWUserBuckets)

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets, bool need_stats);

/**
 * Store the set of buckets associated with a user.
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
extern int rgw_write_buckets_attr(RGWRados *store, string user_id, RGWUserBuckets& buckets);

extern int rgw_add_bucket(RGWRados *store, string user_id, rgw_bucket& bucket);
extern int rgw_remove_user_bucket_info(RGWRados *store, string user_id, rgw_bucket& bucket);

extern int rgw_remove_object(RGWRados *store, rgw_bucket& bucket, std::string& object);
extern int rgw_remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children);

extern void check_bad_user_bucket_mapping(RGWRados *store, const string& user_id, bool fix);

struct RGWBucketAdminOpState {
  std::string uid;
  std::string display_name;
  std::string bucket_name;
  std::string bucket_id;
  std::string object_name;
  const char *etag;

  bool list_buckets;
  bool stat_buckets;
  bool check_objects;
  bool fix_index;
  bool delete_child_objects;
  bool bucket_stored;
  bool match_etag;
  bool check_unmodified;
  bool fetch_data;
  bool valid_byte_range;
  bool partial_content; // so we don't try and dereference a null pointer

  off_t ofs;
  off_t end;
  time_t *check_time;
  time_t lastmod;

  uint64_t read_len;
  uint64_t obj_size;
  uint64_t object_data_length;
  uint64_t epoch;

  rgw_bucket bucket;
  rgw_obj object;
  RGWUserBuckets buckets;

  bufferlist object_bl;
  std::map<std::string, bufferlist> object_attrs;
  std::map<std::string, std::string> response_attrs_params;
  std::list< std::pair<bufferlist, off_t> > object_data;

  void *handle;

  bufferlist& get_object_data() {
    if (object_bl.length() > 0) {
      return object_bl;
    } else if (!object_data.empty()) {
      std::list< std::pair<bufferlist, off_t> >::iterator packet_it;
      packet_it = object_data.begin();
      object_data_length = 0;
      object_bl.clear();

      for (; packet_it != object_data.end(); ++packet_it) {
        std::pair<bufferlist, off_t> packet = *packet_it;
        bufferlist packet_data = packet.first;
        off_t data_len = packet.second;

        object_bl.append(packet_data.c_str(), data_len);
        object_data_length += data_len;
      }
    }

    return object_bl;
  }

  std::list< std::pair<bufferlist, off_t> >& get_raw_object_data() {
    return object_data;
  }

  void append_object_data(std::pair<bufferlist, off_t>& packet) {
    object_data.push_back(packet);
  }

  void clear_object_data() {
    object_bl.zero();
    object_data.clear();
    object_data_length = 0;
  }

  std::map<std::string, std::string>& get_response_attr_params() {
    return response_attrs_params;
  }

  void set_obj_read_len(uint64_t size) { read_len = size; };
  void set_obj_size(uint64_t size) { obj_size = size; };
  void set_read_offset(off_t _ofs) {
    if (_ofs >= 0)
      ofs = _ofs;
  }
  void set_end_read_pos(off_t _end) {
    if (_end >= 0)
      end = _end;
  }
  void set_lastmod(time_t t) { lastmod = t; };
  void set_epoch(uint64_t e) { epoch = e; };

  off_t get_read_offset() { return ofs; };
  off_t get_end_read_pos() { return end; };
  off_t get_object_data_length() { return object_data_length; }
  time_t get_lastmod() { return lastmod; };
  uint64_t get_epoch() { return epoch; };
  uint64_t get_read_len() { return read_len; };
  size_t get_obj_size() { return obj_size; };

  void set_fetch_stats(bool value) { stat_buckets = value; };
  void set_check_objects(bool value) { check_objects = value; };
  void set_fix_index(bool value) { fix_index = value; };
  void set_delete_children(bool value) { delete_child_objects = value; };
  void set_fetch_data(bool fetch) { fetch_data = fetch; };

  void set_user_id(std::string& user_id) {
    if (!user_id.empty())
      uid = user_id;
  }
  void set_bucket_name(std::string& bucket_str) {
    if (!bucket_str.empty())
      bucket_name = bucket_str;
  }
  void set_object_name(std::string& object_str) {
    if (!object_str.empty())
      object_name = object_str;
  }

  std::string& get_user_id() { return uid; };
  std::string& get_user_display_name() { return display_name; };
  std::string& get_bucket_name() { return bucket_name; };
  std::string& get_object_name() { return object_name; };
  const char *get_etag() { return etag; };

  rgw_bucket& get_bucket() { return bucket; };
  void set_bucket(rgw_bucket& _bucket) {
    bucket = _bucket; 
    bucket_stored = true;
  }

  RGWUserBuckets& get_user_buckets() { return buckets; };
  void set_user_buckets(RGWUserBuckets& _buckets) { buckets = _buckets; };

  rgw_obj& get_object() {
    if (!object.key.empty())
      return object;
    else if (!object_name.empty() && bucket_stored)
      object.init(bucket, object_name);

    return object;
  };

  bufferlist& get_object_bl() { return object_bl; };
  std::map<std::string, bufferlist>& get_object_attrs() { return object_attrs; };

  void set_check_time(const char *mtime, bool if_unmodified) {
    parse_time(mtime, check_time);
    check_unmodified = if_unmodified;
  }
  void set_check_etag(const char *tag, bool match) {
    match_etag = match;
    etag = tag;
  }
  void set_read_range(const char *range) {
    parse_range(range, ofs, end, &partial_content);
  }

  time_t *get_check_time() { return check_time; };
  time_t get_mod_time() { return lastmod; };

  bool will_fetch_stats() { return stat_buckets; };
  bool will_fix_index() { return fix_index; };
  bool will_delete_children() { return delete_child_objects; };
  bool will_check_objects() { return check_objects; };
  bool is_user_op() { return !uid.empty(); };
  bool is_system_op() { return uid.empty(); }; 
  bool has_bucket_stored() { return bucket_stored; };
  bool etag_must_match() { return (etag && match_etag); };
  bool etag_must_not_match() { return (etag && !match_etag); };
  bool will_check_modified() { return (check_time && !check_unmodified); };
  bool will_check_unmodified() { return (check_time && check_unmodified); };
  bool will_fetch_data() { return fetch_data; };
  bool will_dump_range() { return (valid_byte_range && partial_content); };

  void **get_handle() { return &handle; };

  RGWBucketAdminOpState() : list_buckets(false), stat_buckets(false), check_objects(false),
                            fix_index(false), delete_child_objects(false),
                            bucket_stored(false), match_etag(false),
                            check_unmodified(false), fetch_data(false),
                            valid_byte_range(false), partial_content(false),
                            check_time(NULL), handle(NULL)  {}
};

/*
 * A simple wrapper class for administrative bucket operations
 */

class RGWBucket
{
  RGWUserBuckets buckets;
  RGWRados *store;
  RGWAccessHandle handle;

  std::string user_id;
  std::string bucket_name;

  bool failure;

private:

public:
  RGWBucket() : store(NULL), failure(false) {}
  int init(RGWRados *storage, RGWBucketAdminOpState& op_state);

  int create_bucket(string bucket_str, string& user_id, string& display_name);
  
  int check_bad_index_multipart(RGWBucketAdminOpState& op_state,
          list<std::string>& objs_to_unlink, std::string *err_msg = NULL);

  int check_object_index(RGWBucketAdminOpState& op_state,
          map<string, RGWObjEnt> result, std::string *err_msg = NULL);

  int check_index(RGWBucketAdminOpState& op_state,
          map<RGWObjCategory, RGWBucketStats>& existing_stats,
          map<RGWObjCategory, RGWBucketStats>& calculated_stats,
          std::string *err_msg = NULL);

  int remove(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int link(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int unlink(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);

  int remove_object(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int get_policy(RGWBucketAdminOpState& op_state, ostream& o);

  int get_object_head(RGWBucketAdminOpState& op_state);
  int iterate_object(RGWBucketAdminOpState& op_state);
  int stat_object(RGWBucketAdminOpState& op_state);
  int read_object(RGWBucketAdminOpState& op_state);
  int get_object_simple(RGWBucketAdminOpState& op_state);
  int get_object(RGWBucketAdminOpState& op_state);

  void clear_failure() { failure = false; };
};

class RGWBucketAdminOp
{
public:
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  ostream& os);


  static int unlink(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int link(RGWRados *store, RGWBucketAdminOpState& op_state);

  static int check_index(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int remove_bucket(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int get_object(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int remove_object(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int info(RGWRados *store, RGWBucketAdminOpState& op_state, RGWFormatterFlusher& flusher);
};

#endif
