# resolve links - $0 may be a softlink

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

. "$bin"/setenv.sh


# the root of the HONU installation
export HONU_HOME=`dirname "$this"`/..
pidfile="${HONU_HOME}/logs/collector.pid"

stop() {
    echo -n $"Stopping collector "
	test -f $pidfile && kill `cat $pidfile`
        RETVAL=$?
        if [ $RETVAL = 0 ]; then
		rm -f ${pidfile}
		echo "collector stopped."
	else
		echo "collector failed to stop"
	fi
        echo
        return $RETVAL
}


if [ "X$1" = "Xstop" ]; then
   stop
fi

if [ -d ${HONU_HOME} ]; then
 mkdir -p ${HONU_HOME}/logs
fi 


HONU_STREAMING_JAR=`ls ${HONU_HOME}/dist/*.jar`
export HONU_STREAMING_JAR=`echo ${HONU_STREAMING_JAR} | sed 'y/ /:/'`

HONU_DEP_JARS=`ls ${HONU_HOME}/lib//*.jar`
export HONU_DEP_JARS=`echo ${HONU_DEP_JARS} | sed 'y/ /:/'`


export JAVA_LIB_PATH="${HONU_HOME}/lib/lz0/native/Linux-i386-32/"

nohup java ${JAVA_OPS}  -Djava.library.path=${JAVA_LIB_PATH} -classpath ${HONU_STREAMING_JAR}:${HONU_DEP_JARS} org.honu.datacollection.collector.streaming.ThriftTHsHaCollector 7101 > ${HONU_HOME}/logs/collector-stdout.log 2>&1 &
echo $! > $pidfile



