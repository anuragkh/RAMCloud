sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$sbin/../obj.master"

rm -f server.log
nohup $sbin/../obj.master/server -C tcp:host=0.0.0.0,port=11211 -L tcp:host=0.0.0.0,port=1101 -r 0 --allowLocalBackup --maxNonVolatileBuffers=0 --totalMasterMemory=20000 --logFile server.log >/dev/null 2>/dev/null &
