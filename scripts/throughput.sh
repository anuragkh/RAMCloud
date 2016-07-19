hostname=${1:-localhost}

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

ssh -o StrictHostKeyChecking=no $hostname "bash /home/ec2-user/RAMCloud/scripts/stop-all.sh"

for i in 1 2 4 8 16 32 64 128 256; do
  for mix in 0.0-0.0-1.0-0.0 0.34-0.33-0.33-0.00 0.5-0.5-0.0-0.0; do
    echo "Running $i, $mix"
    ssh -o StrictHostKeyChecking=no $hostname "bash /home/ec2-user/RAMCloud/scripts/start-all.sh"
    sleep 5
    $sbin/../bench/rbench -b throughput-$mix -a 16 -n $i ~/data 2>&1 | tee log-${mix}-$i.txt
    ssh -o StrictHostKeyChecking=no $hostname "bash /home/ec2-user/RAMCloud/scripts/stop-all.sh"
    sleep 5
  done
done
