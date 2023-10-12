#!/bin/bash -l
#SBATCH -J SparkJob_5_nodes
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH -o slurm-%j.log


### Slightly adapted from: https://hpc.uni.lu/users/docs/slurm_launchers.html#apache-spark
### USEFUL GUIDE: https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html

### RUN this for export environment
### conda create -y -n pyspark_conda_env -c conda-forge pyarrow pandas conda-pack
### conda activate pyspark_conda_env
### conda pack -f -o pyspark_conda_env.tar.gz


### If you do not wish tmp dirs to be cleaned
### at the job end, set below to 0
export SPARK_CLEAN_TEMP=1

### START INTERNAL CONFIGURATION

## CPU and Memory settings
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK}
export DAEMON_MEM=4096
export NODE_MEM=$((4096*${SLURM_CPUS_PER_TASK}-${DAEMON_MEM}))
export SPARK_DAEMON_MEMORY=${DAEMON_MEM}m
export SPARK_NODE_MEM=${NODE_MEM}m

## MY GLOBAL VAR
export FILE_DIR=LearningWithMassiveData/Assignment3/
export FILE_NAME="test.py"
export ENV_NAME="lwmd_ass3.tar.gz"
export PYSPARK_PYTHON="/rhome/lambda01/mambaforge/envs/lwmd_ass3/bin/python"
export SPARK_HOME="$HOME/$FILE_DIR/spark-3.4.0-bin-hadoop3"


## Set up job directories and environment variables
export SPARK_JOB="$HOME/spark-jobs/${SLURM_JOBID}"
mkdir -p "${SPARK_JOB}"


export SPARK_WORKER_DIR=${SPARK_JOB}
export SPARK_LOCAL_DIRS=${SPARK_JOB}
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=9080
export SPARK_WORKER_WEBUI_PORT=9081
export SPARK_INNER_LAUNCHER=${SPARK_JOB}/spark-start-all.sh
export SPARK_MASTER_FILE=${SPARK_JOB}/spark_master

export HADOOP_HOME_WARN_SUPPRESS=1
export HADOOP_ROOT_LOGGER="WARN,DRFA"

export SPARK_SUBMIT_OPTIONS="--conf spark.executor.memory=${SPARK_NODE_MEM} --conf spark.python.worker.memory=${SPARK_NODE_MEM} --archives $HOME/$FILE_DIR/$ENV_NAME#environment"

## Generate spark starter-script
cat << 'EOF' > ${SPARK_INNER_LAUNCHER}
#!/bin/bash
## Load configuration and environment
source "$SPARK_HOME/sbin/spark-config.sh"
source "$SPARK_HOME/bin/load-spark-env.sh"
if [[ ${SLURM_PROCID} -eq 0 ]]; then
    ## Start master in background
    export SPARK_MASTER_HOST=$(hostname)
    MASTER_NODE=$(scontrol show hostname ${SLURM_NODELIST} | head -n 1)

    echo "spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}" > "${SPARK_MASTER_FILE}"

    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
        --ip $SPARK_MASTER_HOST                                           \
        --port $SPARK_MASTER_PORT                                         \
        --webui-port $SPARK_MASTER_WEBUI_PORT &

    ## Start one worker with one less core than the others on this node
    export SPARK_WORKER_CORES=$((${SLURM_CPUS_PER_TASK}-1))
    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
       --webui-port ${SPARK_WORKER_WEBUI_PORT}                             \
       spark://${MASTER_NODE}:${SPARK_MASTER_PORT} &

    ## Wait for background tasks to complete
    wait
else
    ## Start (pure) worker
    MASTER_NODE=spark://$(scontrol show hostname $SLURM_NODELIST | head -n 1):${SPARK_MASTER_PORT}
    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
       --webui-port ${SPARK_WORKER_WEBUI_PORT}                             \
       ${MASTER_NODE}
fi
EOF
chmod +x ${SPARK_INNER_LAUNCHER}

## Launch SPARK and wait for it to start
srun ${SPARK_INNER_LAUNCHER} &
while [ -z "$MASTER" ]; do
        sleep 5
        MASTER=$(cat "${SPARK_MASTER_FILE}")
done
### END OF INTERNAL CONFIGURATION

### USER CODE EXECUTION
OUTPUTFILE=${SLURM_JOB_NAME}-${SLURM_JOB_ID}.out
${SPARK_HOME}/bin/spark-submit ${SPARK_SUBMIT_OPTIONS} --master $MASTER $HOME/$FILE_DIR/$FILE_NAME > ${OUTPUTFILE}

### FINAL CLEANUP
if [[ -n "${SPARK_CLEAN_TEMP}" && ${SPARK_CLEAN_TEMP} -eq 1 ]]; then
    echo "====== Cleaning up: SPARK_CLEAN_TEMP=${SPARK_CLEAN_TEMP}"
    rm -rf ${SPARK_JOB}
else
    echo "====== Not cleaning up: SPARK_CLEAN_TEMP=${SPARK_CLEAN_TEMP}"
fi