hostname=${1:-localhost}
dataset=${2:-tpch}

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

if [ "$dataset" = "tpch" ]; then
  num_attr=16
elif [ "$dataset" = "conviva" ]; then
  num_attr=104
else 
  echo "Invalid dataset"
  exit -1
fi

ssh -o StrictHostKeyChecking=no $hostname "/home/ec2-user/RAMCloud/scripts/stop-all.sh"

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/home/ec2-user/RAMCloud/obj.master"

echo "Running benchmark on server $hostname for dataset $dataset"

delete=0.00
for search in 0.00 0.10 0.20 0.30 0.40 0.50 0.60 0.70 0.80 0.90 1.00; do
  remaining=`echo "scale=2;1.00-$search" | bc` 
  for type in mix; do
    if [ "$type" = "get" ]; then
      get=`printf "%0.2f" $remaining`
      insert=0.00
    elif [ "$type" = "insert" ]; then
      get=0.00
      insert=`printf "%0.2f" $remaining`
    elif [ "$type" = "mix" ]; then
      half=`echo "scale=2;$remaining*0.50" | bc`
      get=`printf "%0.2f" $half`
      insert=`printf "%0.2f" $half`
    fi
    mix=${get}-${search}-${insert}-${delete}
    for i in 2 4 8 16 32; do
      echo "Running $i, get=[$get], search=[$search], insert=[$insert], delete=[$delete]"
      ssh -o StrictHostKeyChecking=no $hostname "/home/ec2-user/RAMCloud/scripts/start-all.sh"
      sleep 5
      echo "Starting benchmark..."
      $sbin/../bench/rbench -b throughput-$mix -a $num_attr -n $i -h $hostname ~/$dataset 2>&1 | tee log_${get}_${search}_${insert}_${delete}_${i}.txt
      ssh -o StrictHostKeyChecking=no $hostname "/home/ec2-user/RAMCloud/scripts/stop-all.sh"
      sleep 5
    done
  done
done
