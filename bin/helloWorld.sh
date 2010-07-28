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

# current directory
export CUR_DIR=`dirname "$this"`/..


HONU_CLIENT_JAR="${CUR_DIR}/dist/honu-client-streaming.jar"
HONU_EXAMPLE_JAR="${CUR_DIR}/dist/honu-example.jar"

HONU_DEP_JARS=`ls ${CUR_DIR}/lib/*.jar`
export HONU_DEP_JARS=`echo ${HONU_DEP_JARS} | sed 'y/ /:/'`


java -Dlog4j.configuration="file:///${CUR_DIR}/conf/honu-log4j-client.properties" -Dlog4j.debug=true -classpath ${CUR_DIR}/conf:${HONU_EXAMPLE_JAR}:${HONU_CLIENT_JAR}:${HONU_DEP_JARS} org.honu.example.client.HelloWorld
