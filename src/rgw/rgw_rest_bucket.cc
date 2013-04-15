#include "rgw_op.h"
#include "rgw_bucket.h"
#include "rgw_rest_bucket.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_Bucket_Info : public RGWRESTOp {

public:
  RGWOp_Bucket_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "get_bucket_info"; }
};

void RGWOp_Bucket_Info::execute()
{
  RGWBucketAdminOpState op_state;

  bool fetch_stats;

  std::string uid;
  std::string bucket;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "stats", false, &fetch_stats);


  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);
  op_state.set_fetch_stats(fetch_stats);

  http_ret = RGWBucketAdminOp::info(store, op_state, flusher);
}

class RGWOp_Get_Policy : public RGWRESTOp {

public:
  RGWOp_Get_Policy() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "get_policy"; }
};

void RGWOp_Get_Policy::execute()
{
  RGWBucketAdminOpState op_state;

  std::string bucket;
  std::string object;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object_name(object);

  http_ret = RGWBucketAdminOp::get_policy(store, op_state, flusher);
}

class RGWOp_Check_Bucket_Index : public RGWRESTOp {

public:
  RGWOp_Check_Bucket_Index() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "check_bucket_index"; }
};

void RGWOp_Check_Bucket_Index::execute()
{
  std::string bucket;

  bool fix_index;
  bool check_objects;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "fix", false, &fix_index);
  RESTArgs::get_bool(s, "check-objects", false, &check_objects);

  op_state.set_bucket_name(bucket);
  op_state.set_fix_index(fix_index);
  op_state.set_check_objects(check_objects);

  http_ret = RGWBucketAdminOp::check_index(store, op_state, flusher);
}

class RGWOp_Bucket_Link : public RGWRESTOp {

public:
  RGWOp_Bucket_Link() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "link_bucket"; }
};

void RGWOp_Bucket_Link::execute()
{
  std::string uid;
  std::string bucket;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "bucket", bucket, &bucket);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);

  http_ret = RGWBucketAdminOp::link(store, op_state);
}

class RGWOp_Bucket_Unlink : public RGWRESTOp {

public:
  RGWOp_Bucket_Unlink() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "unlink_bucket"; }
};

void RGWOp_Bucket_Unlink::execute()
{
  std::string uid;
  std::string bucket;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "bucket", bucket, &bucket);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);

  http_ret = RGWBucketAdminOp::unlink(store, op_state);
}

class RGWOp_Bucket_Remove : public RGWRESTOp {

public:
  RGWOp_Bucket_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "remove_bucket"; }
};

void RGWOp_Bucket_Remove::execute()
{
  std::string bucket;
  bool delete_children;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "purge-objects", false, &delete_children);

  op_state.set_bucket_name(bucket);
  op_state.set_delete_children(delete_children);

  http_ret = RGWBucketAdminOp::remove_bucket(store, op_state);
}

class RGWOp_Object_Remove: public RGWRESTOp {

public:
  RGWOp_Object_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "remove_object"; }
};

void RGWOp_Object_Remove::execute()
{
  std::string bucket;
  std::string object;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object_name(object);

  http_ret = RGWBucketAdminOp::remove_object(store, op_state);
}

class RGWOp_Get_Object: public RGWRESTOp {
  bool fetch_data;

public:
  RGWOp_Get_Object(bool fetch) : fetch_data(fetch) {}
  RGWOp_Get_Object() : fetch_data(false) {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "get_object"; }
};

void RGWOp_Get_Object::execute()
{
  std::string bucket_name;
  std::string object_name;
  bool match_etag;
  bool check_modified;

  rgw_bucket bucket;
  RGWBucketInfo bucket_info;
  RGWBucketAdminOpState op_state;

  std::string etag;
  std::string check_date;
  const char *if_match = NULL;
  const char *if_nomatch = NULL;
  int64_t ofs = -1;
  int64_t end = -1;
  time_t *lastmod;
  uint64_t *total_size;
  uint64_t *obj_size;

  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "object", object_name, &object_name);
  RESTArgs::get_string(s, "etag", etag, &etag);
  RESTArgs::get_string(s, "check-date", check_date, &check_date); 
  RESTArgs::get_int64(s, "offset", ofs, &ofs);
  RESTArgs::get_int64(s, "end", end, &end);
  RESTArgs::get_bool(s, "match-etag", false, &match_etag); 
  RESTArgs::get_bool(s, "check_modified", false, &check_modified);

  op_state.set_fetch_data(fetch_data);
  op_state.set_read_offset(ofs);
  op_state.set_end_read_pos(end);
  op_state.set_bucket_name(bucket_name);
  op_state.set_object_name(object_name);
  op_state.set_check_etag(etag, match_etag);
  op_state.set_check_time(check_date.c_str(), check_modified);

}

RGWOp *RGWHandler_Bucket::op_get()
{

  if (s->args.sub_resource_exists("policy"))
    return new RGWOp_Get_Policy;

  if (s->args.sub_resource_exists("index"))
    return new RGWOp_Check_Bucket_Index;

  return new RGWOp_Bucket_Info;
};

RGWOp *RGWHandler_Bucket::op_put()
{
  return new RGWOp_Bucket_Link;
};

RGWOp *RGWHandler_Bucket::op_post()
{
  return new RGWOp_Bucket_Unlink;
};

RGWOp *RGWHandler_Bucket::op_delete()
{
  if (s->args.sub_resource_exists("object"))
    return new RGWOp_Object_Remove;

  return new RGWOp_Bucket_Remove;
};

