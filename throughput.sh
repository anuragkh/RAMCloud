for i in 4 8 16 32 64; do
  for mix in 0.0-0.0-1.0-0.0; do #0.34-0.33-0.33-0.00 0.5-0.5-0.0-0.0; do
    echo "Running $i, $mix"
    bash start-all.sh
    sleep 5
    bench/rbench -b throughput-$mix -a 16 -n $i ~/data 2>&1 | tee log-${mix}-$i.txt
    bash stop-all.sh
    sleep 5
  done
done
