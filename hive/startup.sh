#!/bin/bash

set -Eeuo pipefail

hadoop fs -mkdir -p /tmp /user/hive/warehouse
hadoop fs -chmod 1777 /tmp
hadoop fs -chmod 777 /user/hive/warehouse

if ! ./schematool -dbType derby -info >/dev/null 2>&1; then
  ./schematool -dbType derby -initSchema
fi

exec ./hiveserver2 --hiveconf hive.server2.enable.doAs=false
