#ifndef CEPH_RGW_REST_ZONE_H
#define CEPH_RGW_REST_ZONE_H

#include "rgw_rest.h"
#include "rgw_rest_s3.h"


class RGWHandler_Zone : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_put();
  RGWOp *op_delete();
public:
  RGWHandler_Zone() {}
  virtual ~RGWHandler_Zone() {}

  int read_permissions(RGWOp*) {
    return 0;
  }
};

class RGWRESTMgr_Zone : public RGWRESTMgr {
public:
  RGWRESTMgr_Zone() {}
  virtual ~RGWRESTMgr_Zone() {}

  RGWHandler *get_handler(struct req_state *s) {
    return new RGWHandler_Zone;
  }
};

#endif
