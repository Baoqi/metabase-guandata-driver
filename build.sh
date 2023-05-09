#!/bin/bash
cd $(dirname $0) 
DRIVER_PATH=$(pwd)

cd ../metabase

clojure \
  -Sdeps '{:aliases {:guandata { :extra-deps {com.guandata.sql/guandata-jdbc {:local/root "'${DRIVER_PATH}'/jdbc_driver/guandata-jdbc-2.0.0-SNAPSHOT.jar"} com.metabase/guandata-driver {:local/root "'${DRIVER_PATH}'"} }}}}'  \
  -X:build:guandata \
  build-drivers.build-driver/build-driver! \
  "{:driver :guandata, :project-dir \"${DRIVER_PATH}\", :target-dir \"${DRIVER_PATH}/target\"}"

cd "${DRIVER_PATH}"

