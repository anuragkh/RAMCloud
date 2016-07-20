sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$sbin/../obj.master"
ip=`wget -qO- http://instance-data/latest/meta-data/public-ipv4`

rm -f server.log
nohup $sbin/../obj.master/server -C tcp:host=${ip},port=11211 -L tcp:host=${ip},port=11101 -r 0 --allowLocalBackup --maxNonVolatileBuffers=0 --totalMasterMemory=20000 --logFile server.log >/dev/null 2>/dev/null &
