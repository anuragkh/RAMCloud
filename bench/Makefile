rbench: RAMCloudBenchmark.cc RAMCloudBenchmark.h
	g++ -g -O3 -std=c++11 -L../obj.master -lramcloud -I../src -I../obj.master -o rbench RAMCloudBenchmark.cc
	
rload: Load.cc
	g++ -g -O3 -std=c++11 -L../obj.master -lramcloud -I../src -I../obj.master -o rload Load.cc

rload: PacketLoader.cc PacketLoader.h
	g++ -g -O3 -std=c++11 -L../obj.master -lramcloud -I../src -I../obj.master -o pload PacketLoader.cc

clean:
	rm -f rbench
