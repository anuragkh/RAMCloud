rm -f server.log
obj.master/server -C tcp:host=0.0.0.0,port=11211 -L tcp:host=0.0.0.0,port=1101 -r 0 --allowLocalBackup --maxNonVolatileBuffers=0 --totalMasterMemory=60000 --logFile server.log &
