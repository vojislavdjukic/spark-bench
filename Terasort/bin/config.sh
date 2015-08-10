#!/bin/bash

this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
if [ -f "${bin}/../conf/env.sh" ]; then
  set -a
  . "${bin}/../conf/env.sh"
  set +a
fi

APP=Terasort
APP_DIR=${DATA_HDFS}/${APP}
INPUT_HDFS=${DATA_HDFS}/${APP}/Input
OUTPUT_HDFS=${DATA_HDFS}/${APP}/Output
if [ ${COMPRESS_GLOBAL} -eq 1 ]; then
    INPUT_HDFS=${INPUT_HDFS}-comp
    OUTPUT_HDFS=${OUTPUT_HDFS}-comp
fi

# either stand alone or yarn cluster
APP_MASTER=${SPARK_MASTER}

set_gendata_opt
set_run_opt

#input benreport
function print_config(){
	local output=$1

	CONFIG=
	if [ ! -z "$SPARK_STORAGE_MEMORYFRACTION" ]; then
	  CONFIG="${CONFIG} memoryFraction ${SPARK_STORAGE_MEMORYFRACTION}"
	fi
	if [ "$MASTER" = "yarn" ]; then
	  if [ ! -z "$SPARK_EXECUTOR_INSTANCES" ]; then
	    CONFIG="${CONFIG} nmem ${SPARK_EXECUTOR_INSTANCES}"
	  fi
	  if [ ! -z "$SPARK_EXECUTOR_CORES" ]; then
	    CONFIG="${CONFIG} exe_core ${SPARK_EXECUTOR_CORES}"
	  fi
	  if [ ! -z "$SPARK_DRIVER_MEMORY" ]; then
	    CONFIG="${CONFIG} dmem ${SPARK_DRIVER_MEMORY}"
	  fi
	fi
	if [ ! -z "$SPARK_EXECUTOR_MEMORY" ]; then
	  CONFIG="${CONFIG} exe_mem ${SPARK_EXECUTOR_MEMORY}"
	fi

	echo "${APP}_config \
	numV ${numV} numPar ${NUM_OF_PARTITIONS} mu ${mu} sigma ${sigma} \
	numiter ${MAX_ITERATION} tol ${TOLERANCE} reset_prob ${RESET_PROB} \
	${CONFIG}" >> ${output}
}