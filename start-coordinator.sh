rm -f coordinator.log
obj.master/coordinator -C tcp:host=0.0.0.0,port=11211 --logFile coordinator.log &

