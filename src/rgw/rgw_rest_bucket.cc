#include "rgw_op.h"
#include "rgw_bucket.h"
#include "rgw_rest_bucket.h"

#include "rgw_common.h"
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
  void send_response_data(struct req_state *s, RGWBucketAdminOpState& op_state);

  virtual const char *name() { return "get_object"; }
};

void RGWOp_Get_Object::send_response_data(struct req_state *s,
        RGWBucketAdminOpState& op_state)
{
  const char *content_type = NULL;
  std::string content_type_str;
  off_t start = op_state.get_read_offset();
  off_t end = op_state.get_end_read_pos();
  uint64_t size = op_state.get_obj_size();
  uint64_t read_len = op_state.get_read_len();
  time_t lastmod = op_state.get_lastmod();

  std::map<std::string, bufferlist> attrs;
  std::map<std::string, std::string> response_attrs;
  std::map<std::string, std::string> response_attrs_params;
  std::map<std::string, std::string>::iterator riter;
  std::map<std::string, std::string>::iterator param_iter;
  std::map<std::string, bufferlist>::iterator attrs_iter;

  attrs = op_state.get_object_attrs();
  response_attrs_params = op_state.get_response_attr_params();

  bool partial_content = op_state.will_dump_range();

  if (partial_content)
    dump_range(s, start, end, size);

  dump_content_length(s, read_len);
  dump_last_modified(s, lastmod);

  map<std::string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ETAG);
  if (iter != attrs.end()) {
    bufferlist& bl = iter->second;
    if (bl.length()) {
      char *etag = bl.c_str();
      dump_etag(s, etag);
    }
  }

  if (!response_attrs_params.empty()) {
    for (const struct response_attr_param *p = rgw_resp_attr_params; p->param; p++) {
      param_iter = response_attrs_params.find(p->param);
      if (param_iter != response_attrs_params.end()) {
        if (strcmp(p->param, "response-content-type") != 0)
         response_attrs[p->http_attr] = param_iter->second;
        else
          content_type = param_iter->second.c_str();
      }
    }
  }  

  for (attrs_iter = attrs.begin(); attrs_iter != attrs.end(); ++attrs_iter) {
    const char *name = attrs_iter->first.c_str();
    map<string, string>::iterator aiter = rgw_to_http_attrs.find(name);
    if (aiter != rgw_to_http_attrs.end()) {
      if (response_attrs.count(aiter->second) > 0) // was already overridden by a response param
        continue;

      if ((!content_type) && aiter->first.compare(RGW_ATTR_CONTENT_TYPE) == 0) { // special handling for content_type
        content_type = attrs_iter->second.c_str();
        continue;
      }
      response_attrs[aiter->second] = attrs_iter->second.c_str();
    } else {
      if (strncmp(name, RGW_ATTR_META_PREFIX, sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
        name += sizeof(RGW_ATTR_PREFIX) - 1;
        //s->cio->print("%s: %s\r\n", name, iter->second.c_str());
      }
    }
  }

  // dump either 200 (full object) or 206 (partial content)
  set_req_state_err(s, partial_content ? STATUS_PARTIAL_CONTENT : 0);
  dump_errno(s);

  for (riter = response_attrs.begin(); riter != response_attrs.end(); ++riter) {
    //s->cio->print("%s: %s\n", riter->first.c_str(), riter->second.c_str());
  }

  if (!content_type)
    content_type = "binary/octet-stream";
  end_header(s, content_type);

  if (op_state.will_fetch_data()) {
    bufferlist object_bl = op_state.get_object_data();
    off_t object_len = op_state.get_object_data_length();
    //int r = s->cio->write(object_bl.c_str(), object_len);
  }
}

void RGWOp_Get_Object::execute()
{
  std::string bucket_name;
  std::string object_name;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "object", object_name, &object_name);

  op_state.set_fetch_data(fetch_data);
  op_state.set_bucket_name(bucket_name);
  op_state.set_object_name(object_name);

  const char *range_str = s->env->get("HTTP_RANGE");
  const char *if_mod = s->env->get("HTTP_IF_MODIFIED_SINCE");
  const char *if_unmod = if_unmod = s->env->get("HTTP_IF_UNMODIFIED_SINCE");
  const char *if_match = s->env->get("HTTP_IF_MATCH");
  const char *if_nomatch = s->env->get("HTTP_IF_NONE_MATCH");

  if (range_str)
    op_state.set_range(ranage_str):

  if (if_mod)
    op_state.set_check_time(if_mod, false);

  if (if_unmod)
    op_state.set_check_time(if_unmod, true);

  if (if_match)
    op_state.set_check_etag(if_match, true);

  if (if_nomatch)
    op_state,set_check_etag(if_nomatch, false);

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

