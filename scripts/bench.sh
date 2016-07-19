for bench_type in "latency-append" "latency-search"; do
  bash start-all.sh
  sleep 5
  bench/rbench -b $bench_type -a 16 ~/data
  bash stop-all.sh
  sleep 5
done
