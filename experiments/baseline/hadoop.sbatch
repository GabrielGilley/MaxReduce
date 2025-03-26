#!/bin/bash

###################
## Slurm parameters

#SBATCH --ntasks-per-node=1
#SBATCH -o log/%j.out
#SBATCH --exclusive
#SBATCH -c 4
#####################

# TODO #FIXME i modified the HADOOP_WORKERS variable in the start-dfs.sh script
# to unset it. What's the better way to do this?

SCRIPT_PATH=$(scontrol show job $SLURM_JOBID | awk -F= '/Command=/{print $2}')
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

export HADOOP_HOME="$SCRIPT_DIR/hadoop-3.4.1"
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_WORKERS="/tmp/workers"

CONFIG_MARKER_STR="# SLURM SPECIFIC CONFIG"
sed -i "/$CONFIG_MARKER_STR/,\$d" $HADOOP_CONF_DIR/hadoop-env.sh
echo "$CONFIG_MARKER_STR" >> $HADOOP_CONF_DIR/hadoop-env.sh

echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk" >> $HADOOP_CONF_DIR/hadoop-env.sh
echo "export HADOOP_HOME=$SCRIPT_DIR/hadoop-3.4.1" >> $HADOOP_CONF_DIR/hadoop-env.sh
echo "export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop" >> $HADOOP_CONF_DIR/hadoop-env.sh
echo "export HADOOP_SSH_OPTS='-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s'" >> $HADOOP_CONF_DIR/hadoop-env.sh

scontrol show hostnames $SLURM_JOB_NODELIST > $HADOOP_WORKERS

namenode=$(sed -n '1p' $HADOOP_WORKERS)
echo "Namenode: $namenode"

# cleanup previous run
srun bash -c "jps | awk '{print $1}' | xargs kill -9"
srun bash -c 'rm -rf /tmp/hadoop* /scratch/hadoop-*-dir'

cat << EOL > $HADOOP_CONF_DIR/yarn-site.xml
<?xml version="1.0"?>
<configuration>
	<property>
		<name>yarn.resourcemanager.hostname</name>
		<value>$namenode</value>
	</property>
	<property>
		<name>yarn.resourcemanager.address</name>
		<value>$namenode:8032</value>
	</property>
	<property>
		<name>yarn.nodemanager.resource.memory-mb</name>
		<value>524288</value>
	</property>
	<property>
		<name>yarn.nodemanager.resource.cpu-vcores</name>
		<value>64</value>
	</property>
	<property>
		<name>yarn.scheduler.maximum-allocation-mb</name>
		<value>524288</value> <!-- Maximum memory for a single container (512 GB) -->
	</property>
	<property>
		<name>yarn.scheduler.minimum-allocation-mb</name>
		<value>1024</value> <!-- Minimum memory for a single container (1 GB) -->
	</property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
</configuration>
EOL

cat << EOL > $HADOOP_CONF_DIR/hdfs-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<property>
        <name>dfs.namenode.rpc-address</name>
        <value>$namenode:8020</value>
    </property>
	<property>
		<name>dfs.namenode.name.dir</name>
		<value>file:///scratch/hadoop-namenode-dir</value>
	</property>
	<property>
		<name>dfs.datanode.data.dir</name>
		<value>file:///scratch/hadoop-datanode-dir</value>
	</property>

</configuration>
EOL

cat << EOL > $HADOOP_CONF_DIR/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://$namenode:8020</value>
    </property>
</configuration>
EOL

cat << EOL > $HADOOP_CONF_DIR/mapred-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<property>
		<name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>
	<property>
		<name>yarn.app.mapreduce.am.env</name>
		<value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
	</property>
	<property>
		<name>mapreduce.map.env</name>
		<value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
	</property>
	<property>
		<name>mapreduce.reduce.env</name>
		<value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
	</property>

</configuration>
EOL

# Create Hadoop directories
$HADOOP_HOME/bin/hdfs namenode -format -force


$HADOOP_HOME/bin/yarn --daemon start timelineserver

# Start HDFS
# Can't just run start-dfs.sh anymore, its arguments don't work with our setup
# of workers. So we'll just do everything manually
#HADOOP_WORKERS= $HADOOP_HOME/sbin/start-dfs.sh
HADOOP_LIBEXEC_DIR="${HADOOP_HOME}/libexec"
. "${HADOOP_LIBEXEC_DIR}/hdfs-config.sh"
NAMENODES=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -namenodes 2>/dev/null)

echo "Starting namenodes on [${NAMENODES}]"
HADOOP_WORKERS= hadoop_uservar_su hdfs namenode "${HADOOP_HDFS_HOME}/bin/hdfs" \
    --workers \
    --config "${HADOOP_CONF_DIR}" \
    --hostnames "${NAMENODES}" \
    --daemon start \
    namenode ${nameStartOpt}

echo "Starting datanodes"
hadoop_uservar_su hdfs datanode "${HADOOP_HDFS_HOME}/bin/hdfs" \
    --workers \
    --config "${HADOOP_CONF_DIR}" \
    --daemon start \
    datanode ${dataStartOpt}

SECONDARY_NAMENODES=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -secondarynamenodes 2>/dev/null)
echo "Starting secondary namenodes [${SECONDARY_NAMENODES}]"
HADOOP_WORKERS= hadoop_uservar_su hdfs secondarynamenode "${HADOOP_HDFS_HOME}/bin/hdfs" \
  --workers \
  --config "${HADOOP_CONF_DIR}" \
  --hostnames "${SECONDARY_NAMENODES}" \
  --daemon start \
  secondarynamenode


# Start YARN
$HADOOP_HOME/sbin/start-yarn.sh

sleep 10
$HADOOP_HOME/bin/hdfs dfsadmin -report
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/$USER/input/
# to test:
# ./bin/hdfs dfs -put /cscratch/hadoop-test/input.txt /user/$USER/input/
# ./bin/hdfs dfs -ls input
# time ./bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar  wordcount /cscratch/hadoop-test/input.txt /cscratch/hadoop-test/output/
# time ./bin/yarn --workers jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar  wordcount /user/$USER/input/input.txt /user/$USER/output/

sleep infinity
