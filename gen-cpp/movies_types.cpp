/**
 * Autogenerated by Thrift Compiler (0.14.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "movies_types.h"

#include <algorithm>
#include <ostream>

#include <thrift/TToString.h>

namespace movies {

int _kErrorCodeValues[] = {
  ErrorCode::SE_CONNPOOL_TIMEOUT,
  ErrorCode::SE_THRIFT_CONN_ERROR,
  ErrorCode::SE_UNAUTHORIZED,
  ErrorCode::SE_MEMCACHED_ERROR,
  ErrorCode::SE_MONGODB_ERROR,
  ErrorCode::SE_REDIS_ERROR,
  ErrorCode::SE_THRIFT_HANDLER_ERROR,
  ErrorCode::SE_RABBITMQ_CONN_ERROR
};
const char* _kErrorCodeNames[] = {
  "SE_CONNPOOL_TIMEOUT",
  "SE_THRIFT_CONN_ERROR",
  "SE_UNAUTHORIZED",
  "SE_MEMCACHED_ERROR",
  "SE_MONGODB_ERROR",
  "SE_REDIS_ERROR",
  "SE_THRIFT_HANDLER_ERROR",
  "SE_RABBITMQ_CONN_ERROR"
};
const std::map<int, const char*> _ErrorCode_VALUES_TO_NAMES(::apache::thrift::TEnumIterator(8, _kErrorCodeValues, _kErrorCodeNames), ::apache::thrift::TEnumIterator(-1, nullptr, nullptr));

std::ostream& operator<<(std::ostream& out, const ErrorCode::type& val) {
  std::map<int, const char*>::const_iterator it = _ErrorCode_VALUES_TO_NAMES.find(val);
  if (it != _ErrorCode_VALUES_TO_NAMES.end()) {
    out << it->second;
  } else {
    out << static_cast<int>(val);
  }
  return out;
}

std::string to_string(const ErrorCode::type& val) {
  std::map<int, const char*>::const_iterator it = _ErrorCode_VALUES_TO_NAMES.find(val);
  if (it != _ErrorCode_VALUES_TO_NAMES.end()) {
    return std::string(it->second);
  } else {
    return std::to_string(static_cast<int>(val));
  }
}


ServiceException::~ServiceException() noexcept {
}


void ServiceException::__set_errorCode(const ErrorCode::type val) {
  this->errorCode = val;
}

void ServiceException::__set_message(const std::string& val) {
  this->message = val;
}
std::ostream& operator<<(std::ostream& out, const ServiceException& obj)
{
  obj.printTo(out);
  return out;
}


uint32_t ServiceException::read(::apache::thrift::protocol::TProtocol* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          int32_t ecast0;
          xfer += iprot->readI32(ecast0);
          this->errorCode = (ErrorCode::type)ecast0;
          this->__isset.errorCode = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->message);
          this->__isset.message = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t ServiceException::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("ServiceException");

  xfer += oprot->writeFieldBegin("errorCode", ::apache::thrift::protocol::T_I32, 1);
  xfer += oprot->writeI32((int32_t)this->errorCode);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("message", ::apache::thrift::protocol::T_STRING, 2);
  xfer += oprot->writeString(this->message);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

void swap(ServiceException &a, ServiceException &b) {
  using ::std::swap;
  swap(a.errorCode, b.errorCode);
  swap(a.message, b.message);
  swap(a.__isset, b.__isset);
}

ServiceException::ServiceException(const ServiceException& other1) : TException() {
  errorCode = other1.errorCode;
  message = other1.message;
  __isset = other1.__isset;
}
ServiceException& ServiceException::operator=(const ServiceException& other2) {
  errorCode = other2.errorCode;
  message = other2.message;
  __isset = other2.__isset;
  return *this;
}
void ServiceException::printTo(std::ostream& out) const {
  using ::apache::thrift::to_string;
  out << "ServiceException(";
  out << "errorCode=" << to_string(errorCode);
  out << ", " << "message=" << to_string(message);
  out << ")";
}

const char* ServiceException::what() const noexcept {
  try {
    std::stringstream ss;
    ss << "TException - service has thrown: " << *this;
    this->thriftTExceptionMessageHolder_ = ss.str();
    return this->thriftTExceptionMessageHolder_.c_str();
  } catch (const std::exception&) {
    return "TException - service has thrown: ServiceException";
  }
}

} // namespace
