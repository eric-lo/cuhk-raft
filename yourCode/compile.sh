BASE_PATH=$(cd `dirname $0` && pwd)
BIN_PATH=$BASE_PATH/../bin

# Build the student's raft node implementation.
# Exit immediately if there was a compile-time erro
go mod tidy
go build -o $BIN_PATH/raftrunner $BASE_PATH
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi


