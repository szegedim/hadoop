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

#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <linux/limits.h>

/*
This file implements a standard cgroups out of memory listener.
*/

typedef struct _descriptors {
  int event_fd;
  int event_control_fd;
  int oom_control_fd;
  char event_control_path[PATH_MAX];
  char oom_control_path[PATH_MAX];
  char oom_command[25];
  int oom_command_len;
} __descriptors;
struct _descriptors descriptors = {-1, -1, -1, {0}, {0}, {0}, 0};

/*
 Clean up allocated resources
*/
void cleanup() {
  close(descriptors.event_fd);
  close(descriptors.event_control_fd);
  close(descriptors.oom_control_fd);
}

/*
 Print an error and exit
*/
void error_and_exit(const char* file, const char *message,
                    ...) {
  fprintf(stderr, "%s ", file);
  va_list arguments;
  va_start(arguments, message);
  vfprintf(stderr, message, arguments);
  va_end(arguments);
  cleanup();
  exit(EXIT_FAILURE);
}

void print_usage(void) {
  fprintf(stderr, "usage: oomlistener <cgroup directory>\n");
  cleanup();
  exit(EXIT_FAILURE);
}

/*
 A command that receives a memory cgroup directory and
 listens to the events in the directory.
 It will print a new line on every out of memory event
 to the standard output.
 usage:
 oomlistener <cgroup>
*/
int main(int argc, char *argv[]) {
  if (argc != 2)
    print_usage();

  if ((descriptors.event_fd = eventfd(0, 0)) == -1) {
    error_and_exit(argv[0], "eventfd() failed. errno:%d\n",
                   errno);
  }

  if (snprintf(descriptors.event_control_path,
               sizeof(descriptors.event_control_path), "%s/%s", argv[1],
               "cgroup.event_control") < 0) {
    error_and_exit(argv[0], "path too long %s\n", argv[1]);
  }

  if ((descriptors.event_control_fd = open(
                                        descriptors.event_control_path,
                                        O_WRONLY)) == -1) {
    error_and_exit(argv[0], "Could not open %s. errno:%d\n",
                   descriptors.event_control_path, errno);
  }

  if (snprintf(descriptors.oom_control_path,
               sizeof(descriptors.oom_control_path),
               "%s/%s", argv[1], "memory.oom_control") < 0) {
    error_and_exit(argv[0], "path too long %s\n", argv[1]);
  }

  if ((descriptors.oom_control_fd = open(
                                      descriptors.oom_control_path,
                                      O_RDONLY)) == -1) {
    error_and_exit(argv[0], "Could not open %s. errno:%d\n",
                   descriptors.oom_control_path, errno);
  }

  if ((descriptors.oom_command_len = snprintf(
                                       descriptors.oom_command,
                                       sizeof(descriptors.oom_command),
                                       "%d %d",
                                       descriptors.event_fd,
                                       descriptors.oom_control_fd)) < 0) {
    error_and_exit(argv[0], "Could print %d %d\n",
                   descriptors.event_control_fd,
                   descriptors.oom_control_fd);
  }

  if (write(descriptors.event_control_fd,
            descriptors.oom_command,
            descriptors.oom_command_len) == -1) {
  }

  if (close(descriptors.event_control_fd) == -1) {
    error_and_exit(argv[0], "Could not close %s errno:%d\n",
                   descriptors.event_control_path, errno);
  }
  descriptors.event_control_fd = -1;

  for (;;) {
    uint64_t u;
    int ret = 0;
    if ((ret = read(descriptors.event_fd, &u, sizeof(u))) != sizeof(u)) {
      error_and_exit(argv[0],
                     "Could not read from eventfd %d errno:%d\n", ret, errno);
    }

    printf("oom\n");
  }

  cleanup();

  return 0;
}
