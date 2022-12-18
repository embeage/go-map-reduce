clean:
	rm -f mr-*
	rm -f wc.so

build: clean
	go build -race -buildmode=plugin ../mrapps/wc.go

run_sequential: clean build
	go run -race mrsequential.go wc.so pg*.txt
	cat mr-out-0 | sort | more 

run_coordinator: clean build
	go run -race mrcoordinator.go pg-*.txt

run_worker:
	go run -race mrworker.go wc.so	
