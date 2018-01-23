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
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/eventfd.h>
#include <sys/stat.h>

#include <linux/limits.h>

/*
This file implements a standard cgroups out of memory listener.
*/

typedef struct _descriptors {
    const char* command;
    int event_fd;
    int event_control_fd;
    int oom_control_fd;
    char event_control_path[PATH_MAX];
    char oom_control_path[PATH_MAX];
    char oom_command[25];
    size_t oom_command_len;
} _descriptors;

/*
 Clean up allocated resources in a descriptor structure
*/
inline void cleanup(_descriptors* descriptors) {
    close(descriptors->event_fd);
    descriptors->event_fd = -1;
    close(descriptors->event_control_fd);
    descriptors->event_control_fd = -1;
    close(descriptors->oom_control_fd);
    descriptors->oom_control_fd = -1;
}

/*
 * Enable an OOM listener on the memory cgroup cgroup
 */
int oom_listener(_descriptors* descriptors, const char* cgroup);

#endif
