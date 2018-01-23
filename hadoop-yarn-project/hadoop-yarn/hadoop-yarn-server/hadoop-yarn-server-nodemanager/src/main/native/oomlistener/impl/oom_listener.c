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

#include "oom_listener.h"

/*
 Print an error and exit
*/
static void print_error(const char *file, const char *message,
                 ...) {
  fprintf(stderr, "%s ", file);
  va_list arguments;
  va_start(arguments, message);
  vfprintf(stderr, message, arguments);
  va_end(arguments);
}


int oom_listener(_descriptors* descriptors, const char* cgroup) {
    if ((descriptors->event_fd = eventfd(0, 0)) == -1) {
        print_error(descriptors->command, "eventfd() failed. errno:%d %s\n",
                    errno, strerror(errno));
        return EXIT_FAILURE;
    }

    if (snprintf(descriptors->event_control_path,
                 sizeof(descriptors->event_control_path), "%s/%s", cgroup,
                 "cgroup.event_control") < 0) {
        print_error(descriptors->command, "path too long %s\n", cgroup);
        return EXIT_FAILURE;
    }

    if ((descriptors->event_control_fd = open(
            descriptors->event_control_path,
            O_WRONLY)) == -1) {
        print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                    descriptors->event_control_path,
                    errno, strerror(errno));
        return EXIT_FAILURE;
    }

    if (snprintf(descriptors->oom_control_path,
                 sizeof(descriptors->oom_control_path),
                 "%s/%s", cgroup, "memory.oom_control") < 0) {
        print_error(descriptors->command, "path too long %s\n", cgroup);
        return EXIT_FAILURE;
    }

    if ((descriptors->oom_control_fd = open(
            descriptors->oom_control_path,
            O_RDONLY)) == -1) {
        print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                    descriptors->oom_control_path,
                    errno, strerror(errno));
        return EXIT_FAILURE;
    }

    if ((descriptors->oom_command_len = (size_t) snprintf(
            descriptors->oom_command,
            sizeof(descriptors->oom_command),
            "%d %d",
            descriptors->event_fd,
            descriptors->oom_control_fd)) < 0) {
        print_error(descriptors->command, "Could print %d %d\n",
                    descriptors->event_control_fd,
                    descriptors->oom_control_fd);
        return EXIT_FAILURE;
    }

    if (write(descriptors->event_control_fd,
              descriptors->oom_command,
              descriptors->oom_command_len) == -1) {
    }

    if (close(descriptors->event_control_fd) == -1) {
        print_error(descriptors->command, "Could not close %s errno:%d\n",
                    descriptors->event_control_path, errno);
        return EXIT_FAILURE;
    }
    descriptors->event_control_fd = -1;

    for (;;) {
        uint64_t u;
        ssize_t ret = 0;
        struct stat stat_buffer = {0};

        if ((ret = read(descriptors->event_fd, &u, sizeof(u))) != sizeof(u)) {
            print_error(descriptors->command,
                        "Could not read from eventfd %d errno:%d %s\n", ret,
                        errno, strerror(errno));
            return EXIT_FAILURE;
        }

        printf("oom %ld\n", u);
        if (stat(cgroup, &stat_buffer) != 0) {
            print_error(descriptors->command,
                        "Path deteled: %s errno:%d %s\n", cgroup,
                        errno, strerror(errno));
            return EXIT_FAILURE;
        }
    }
    return EXIT_SUCCESS;
}

#endif
