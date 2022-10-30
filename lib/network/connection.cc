#include "cloudlab/network/connection.hh"
#include "cloudlab/flags.hh"
#include "cloudlab/network/address.hh"

#include "fmt/core.h"

#include "cloud.pb.h"
#include <event2/buffer.h>
#include <event2/bufferevent.h>

#include <netdb.h>
#include <unistd.h>

const size_t MSG_MAX_SIZE = 1024;

namespace cloudlab {

Connection::Connection(const SocketAddress& address) {
  addrinfo hints{}, *req = nullptr;
  memset(&hints, 0, sizeof(addrinfo));

  if (address.is_ipv4()) {
    hints.ai_family = AF_INET;
    hints.ai_addrlen = sizeof(struct sockaddr_in);
  } else {
    hints.ai_family = AF_INET6;
    hints.ai_addrlen = sizeof(struct sockaddr_in6);
  }

  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = IPPROTO_TCP;

  if (getaddrinfo(address.get_ip_address().c_str(),
                  std::to_string(address.get_port()).c_str(), &hints,
                  &req) != 0) {
    throw std::runtime_error("getaddrinfo() failed");
  }

  fd = socket(req->ai_family, req->ai_socktype, req->ai_protocol);
  if (fd == -1) {
    throw std::runtime_error("socket() failed");
  }

  int reuseAddr = 1;
  if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuseAddr, sizeof(int)) != 0){
    throw std::runtime_error("setsockopt() failed: " + std::to_string(errno));
  }

  if(connect(fd, req->ai_addr, req->ai_addrlen) != 0){
    throw std::runtime_error("connect() failed");
  }

  freeaddrinfo(req);
}

Connection::Connection(const std::string& address)
    : Connection(SocketAddress{address}) {
}

Connection::~Connection() {
  close(fd);
}

/***
 * The implementation of sending and receiving messages is flexible. The message
 * syntax specified on cloud.proto and the respective api from protobuff shoud
 * be used. See https://developers.google.com/protocol-buffers/docs/cpptutorial
 */
auto Connection::receive(cloud::CloudMessage& msg) const -> bool {
  char data[MSG_MAX_SIZE];
  ssize_t sz;
  if(fd > 0){
    sz = read(fd, data, MSG_MAX_SIZE);
    if(sz == -1) return false;
  } else {
    struct bufferevent *bufev = static_cast<struct bufferevent*>(bev);
    sz = bufferevent_read(bufev, data, MSG_MAX_SIZE);
  }
  msg.ParseFromArray(data, sz);

  return true;
}

auto Connection::send(const cloud::CloudMessage& msg) const -> bool {
  std::string data;
  msg.SerializeToString(&data);

  if(fd > 0){
    ssize_t sz = write(fd, data.c_str(), data.size());
    return sz == (ssize_t)data.size();
  } else {
    struct bufferevent *bufev = static_cast<struct bufferevent*>(bev);
    int sz = bufferevent_write(bufev, data.c_str(), data.size());
    return sz == (int)data.size();
  }
}

}  // namespace cloudlab
