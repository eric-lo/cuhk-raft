BASE_PATH=$(cd `dirname $0` && pwd)
BIN_PATH=$BASE_PATH/../bin

# Build the student's raft node implementation.
# Exit immediately if there was a compile-time erro
cd $BASE_PATH
mvn package
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

echo "exec java -cp ${BASE_PATH}/target/asgn2-1-jar-with-dependencies.jar csci4160.asgn2.RaftRunner \$1 \$2 \$3 \$4 \$5" > $BIN_PATH/raftrunner
cd --