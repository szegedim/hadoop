/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#if __linux

extern "C" {
#include "oom_listener.h"
}

#include <gtest/gtest.h>
#include <fstream>
#include <mutex>

#define CGROUP_ROOT "/sys/fs/cgroup/memory/"
#define TEST_ROOT "/tmp/test-oom-listener"
#define CGROUP_TASKS "tasks"
#define CGROUP_OOM_CONTROL "memory.oom_control"
#define CGROUP_LIMIT_PHYSICAL "memory.limit_in_bytes"
#define CGROUP_LIMIT_SWAP "memory.memsw.limit_in_bytes"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class OOMListenerTest : public ::testing::Test {
private:
  char cgroup[PATH_MAX] = {};
  std::mutex lock;
public:
  OOMListenerTest() = default;

  virtual ~OOMListenerTest() = default;
  virtual std::mutex& getLock() { return lock; }
  virtual const char* GetCGroup() { return cgroup; }
  virtual void SetUp() {
    const char *cgroup0[] = { CGROUP_ROOT, TEST_ROOT };
    struct stat cgroup_memory = {};
    for (int i = 0; i < GTEST_ARRAY_SIZE_(cgroup); ++i) {
      mkdir(cgroup0[i], 0700);

      if (0 != stat(cgroup0[i], &cgroup_memory)) {
        printf("%s missing. Skipping test", cgroup0[i]);
        continue;
      }

      timespec timespec1 = {};
      if (0 != clock_gettime(CLOCK_MONOTONIC, &timespec1)) {
        ASSERT_TRUE(false) << " clock_gettime failed";
      }

      if (snprintf(cgroup, sizeof(cgroup), "%s/%lx/",
                        cgroup0[i], timespec1.tv_nsec) <= 0) {
        cgroup[0] = '\0';
        printf("%s snprintf failed", cgroup0[i]);
        continue;
      }

      if (0 != mkdir(cgroup, 0700)) {
        printf("%s not writable.", cgroup);
        continue;
      }
      break;
    }

    ASSERT_EQ(0, stat(cgroup, &cgroup_memory))
                  << "Cannot use or simulate cgroup " << cgroup;
  }
  virtual void TearDown() {
    if (cgroup[0] != '\0') {
      rmdir(cgroup);
    }
  }
};

TEST_F(OOMListenerTest, test_oom) {
  // Disable OOM killer
  std::ofstream oom_control;
  std::string oom_control_file = std::string(GetCGroup()).append(CGROUP_OOM_CONTROL);
  oom_control.open(oom_control_file.c_str(), oom_control.out);
  oom_control << 1 << std::endl;
  oom_control.close();

  // Set a low enough limit
  std::ofstream limit;
  std::string limit_file = std::string(GetCGroup()).append(CGROUP_LIMIT_PHYSICAL);
  limit.open(limit_file.c_str(), limit.out);
  limit << 5 * 1024 * 1024 << std::endl;
  limit.close();

  // Set a low enough limit for physical + swap
  std::ofstream limitSwap;
  std::string limit_swap_file = std::string(GetCGroup()).append(CGROUP_LIMIT_SWAP);
  limitSwap.open(limit_swap_file.c_str(), limitSwap.out);
  limitSwap << 5 * 1024 * 1024 << std::endl;
  limitSwap.close();

  std::string tasks_file = std::string(GetCGroup()).append(CGROUP_TASKS);

  getLock().lock();
  __pid_t mem_hog_pid = fork();
  if (!mem_hog_pid) {
    // Child process to consume too much memory

    // Wait until we are added to the cgroup
    __pid_t cgroupPid;
    do {
      std::ifstream tasks;
      tasks.open(tasks_file.c_str(), tasks.in);
      tasks >> cgroupPid;
      tasks.close();
    } while (cgroupPid != getpid());

    const int bufferSize = 1024 * 1024;
    for(;;) {

      auto buffer = (char*)malloc(bufferSize);
      if (buffer == nullptr) {
        break;
      }
      for (int i = 0; i < bufferSize; ++i) {
        buffer[i] = (char)std::rand();
      }
    }
  } else {
    // Parent test
    ASSERT_GE(mem_hog_pid, 1) << "Fork failed " << errno;

    // Put child into cgroup
    std::ofstream tasks;
    tasks.open(tasks_file.c_str(), tasks.out);
    tasks << mem_hog_pid << std::endl;
    tasks.close();

    // Release child
    getLock().unlock();

    int test_pipe[2];
    ASSERT_EQ(0, pipe(test_pipe));

    __pid_t listener = fork();
    if (listener == 0) {
      // child listener forwarding cgroup events
      _oom_listener_descriptors descriptors = {
          .command = "test",
          .event_fd = -1,
          .event_control_fd = -1,
          .oom_control_fd = -1,
          .event_control_path = {0},
          .oom_control_path = {0},
          .oom_command = {0},
          .oom_command_len = 0
      };
      int ret = oom_listener(&descriptors, GetCGroup(), test_pipe[1]);
      cleanup(&descriptors);
      close(test_pipe[0]);
      close(test_pipe[1]);
      exit(ret);
    } else {
      uint64_t event_id;
      ASSERT_EQ(sizeof(event_id), read(test_pipe[0], &event_id, sizeof(event_id)))
                    << "The event has not arrived";
      close(test_pipe[0]);
      close(test_pipe[1]);

      // Simulate OOM killer
      ASSERT_EQ(0, kill(mem_hog_pid, SIGKILL));

      // Verify that process was killed
      __WAIT_STATUS mem_hog_status = {};
      __pid_t exited0 = wait(mem_hog_status);
      ASSERT_EQ(mem_hog_pid, exited0)
                << "Wrong process exited";
      ASSERT_EQ(nullptr, mem_hog_status)
                    << "Test process killed with invalid status";

      // Once the cgroup is empty delete it
      ASSERT_EQ(0, rmdir(GetCGroup()))
                << "Could not delete cgroup " << GetCGroup();

      // Check that oom_listener exited on the deletion of the cgroup
      __WAIT_STATUS oom_listener_status = {};
      __pid_t exited1 = wait(oom_listener_status);
      ASSERT_EQ(listener, exited1)
                    << "Wrong process exited";
      ASSERT_EQ(nullptr, oom_listener_status)
                    << "Listener process exited with invalid status";
    }
  }
}

#else
/*
This tool covers Linux specific functionality,
so it is not available for other operating systems
*/
int main() {
  return 1;
}
#endif
