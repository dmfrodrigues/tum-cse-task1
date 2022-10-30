#include "cloudlab/flags.hh"
#include "cloudlab/network/connection.hh"

#include "cloud.pb.h"

#include "argh.hh"
#include <thread>
#include <fmt/core.h>

using namespace cloudlab;

auto main(int argc, char *argv[]) -> int {
  argh::parser cmdl;
  cmdl.add_params({"-s", "-c", "-m"});
  cmdl.parse(argc, argv);

  std::string serverAddress;
  if(!(cmdl("-s") >> serverAddress)) throw std::invalid_argument("Missing -s");

  int numClients;
  if(!(cmdl("-c") >> numClients)) throw std::invalid_argument("Missing -c");

  int numMessages;
  if(!(cmdl("-m") >> numMessages)) throw std::invalid_argument("Missing -m");

  auto thread_job = [serverAddress, numMessages, numClients](int workerID) -> void {
    auto con = cloudlab::Connection(serverAddress);
    cloud::CloudMessage send_msg{}, receive_msg{};
    send_msg.set_type(cloud::CloudMessage_Type_REQUEST);
    send_msg.set_operation(cloud::CloudMessage_Operation_PUT);
    for(int i = workerID; i < numMessages; i += numClients){
      send_msg.clear_kvp();
      auto *p = send_msg.add_kvp();
      p->set_key(std::to_string(i));
      p->set_value(std::to_string(i));
      con.send(send_msg);
      con.receive(receive_msg);
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(numClients);
  for(int i = 0; i < numClients; ++i)
    threads.emplace_back(thread_job, i);
  for(auto &t: threads)
    t.join();

  return 0;
}
