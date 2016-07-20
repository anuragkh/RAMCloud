sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

echo "Starting coordinator..."
$sbin/start-coordinator.sh
sleep 10
echo "Starting server..."
$sbin/start-server.sh
