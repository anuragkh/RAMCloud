sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$sbin/../obj.master"
ip=`wget -qO- http://instance-data/latest/meta-data/public-ipv4`

rm -f coordinator.log
nohup $sbin/../obj.master/coordinator -C tcp:host=${ip},port=11211 --logFile coordinator.log >/dev/null 2>/dev/null &
