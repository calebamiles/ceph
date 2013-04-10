#include "rgw_op.h"
#include "rgw_user.h"
#include "rgw_rest_zone.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_Zone_Info : public RGWRESTOp {

public:
  RGWOp_Zone_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "get_zone_info"; }
};

void RGWOp_Zone_Info::execute()
{
  http_ret = RGWZoneAdminOp::info(store, flusher);
}

class RGWOp_Add_Pools : public RGWRESTOp {

public:
  RGWOp_Add_Pools() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "add_pools"; }
};

void RGWOp_Add_Pools::execute()
{
  std::string pools;
  bool create;
  RGWZoneAdminOpState op_state;

  RESTArgs::get_string(s, "pools", pools, &pools);
  RESTArgs::get_bool(s, "create", false, &create);

  if (create)
    op_state.set_create_pools();

  op_state.set_pools(pools);

  dump_processing(s); // this may take a long time to complete
  http_ret = RGWZoneAdminOp::add_pools(store, op_state, flusher);
}

class RGWOp_Remove_Pools : public RGWRESTOp {

public:
  RGWOp_Remove_Pools() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "remove_pools"; }
};

void RGWOp_Remove_Pools::execute()
{
  std::string pools;
  RGWZoneAdminOpState op_state;

  RESTArgs::get_string(s, "pools", pools, &pools);
  op_state.set_pools(pools);

  dump_processing(s); // this may take a long time to complete
  http_ret = RGWZoneAdminOp::remove_pools(store, op_state, flusher);
}

class RGWOp_List_Pools : public RGWRESTOp {

public:
  RGWOp_List_Pools() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "list_pools"; }
};

void RGWOp_List_Pools::execute()
{
  http_ret = RGWZoneAdminOp::list_pools(store, flusher);
}

class RGWOp_List_Garbage : public RGWRESTOp {

public:
  RGWOp_List_Garbage() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "list_garbage"; }
};

void RGWOp_List_Garbage::execute()
{
  http_ret = RGWZoneAdminOp::list_garbage(store, flusher);
}

class RGWOp_Process_Garbage : public RGWRESTOp {

public:
  RGWOp_Process_Garbage() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "process_garbage"; }
};

void RGWOp_Process_Garbage::execute()
{
  http_ret = RGWZoneAdminOp::process_garbage(store);
}

class RGWOp_Show_Logs : public RGWRESTOp {

public:
  RGWOp_Show_Logs() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "show_logs"; }
};

void RGWOp_Show_Logs::execute()
{
  std::string object;
  std::string date;
  std::string bucket_id;
  std::string bucket_name;

  RGWZoneAdminOpState op_state;

  RESTArgs::get_string(s, "object" object, &object);
  RESTArgs::get_string(s, "date" date, &date);
  RESTArgs::get_string(s, "bucket_id", bucket_id, &bucket_id);
  RESTArgs::get_string(s, "bucket_name" bucket_name, &bucket_name);

  op_state.set_object(object);
  op_state.set_date(date);
  op_state.set_bucket_id(bucket_id);
  op_state.set_bucket_name(bucket_name);

  http_ret = RGWZoneAdminOp::show_logs(store, op_state, flusher);
}

class RGWOp_Remove_Logs : public RGWRESTOp {

public:
  RGWOp_Remove_Logs() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("zone", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "remove_logs"; }
};

void RGWOp_Remove_Logs::execute()
{
  std::string object;
  std::string date;
  std::string bucket_id;
  std::string bucket_name;

  RGWZoneAdminOpState op_state;

  RESTArgs::get_string(s, "object" object, &object);
  RESTArgs::get_string(s, "date" date, &date);
  RESTArgs::get_string(s, "bucket_id", bucket_id, &bucket_id);
  RESTArgs::get_string(s, "bucket_name" bucket_name, &bucket_name);

  op_state.set_object(object);
  op_state.set_date(date);
  op_state.set_bucket_id(bucket_id);
  op_state.set_bucket_name(bucket_name);

  http_ret = RGWZoneAdminOp::remove_logs(store, op_state);
}

RGWOp *RGWHandler_User::op_get()
{
  if (s->args.sub_resource_exists("pool")
    return new RGWOp_List_Pools;

  if (s->args.sub_resource_exists("log")
    return new RGWOp_Show_Logs;

  if (s->args.sub_resource_exists("garbage")
    return new RGWOp_List_Garbage;

  return new RGWOp_Zone_Info;
};

RGWOp *RGWHandler_User::op_put()
{
  if (s->args.sub_resource_exists("pool")
    return new RGWOp_Add_Pools;

  return NULL;
};

RGWOp *RGWHandler_User::op_delete()
{
  if (s->args.sub_resource_exists("pool"))
    return new RGWOp_Remove_Pools;

  if (s->args.sub_resource_exists("garbage"))
    return new RGWOp_Process_Garbage;

  if (s->args.sub_resource_exists("log"))
    return new RGWOp_Remove_Logs;

  return NULL;
};

