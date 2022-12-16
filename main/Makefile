clean:
	rm -f mr-out*
	rm -f wc.so

build: clean
	go build -race -buildmode=plugin ../mrapps/wc.go

run_sequential: clean build
	go run -race mrsequential.go wc.so pg*.txt
	more mr-out-0

run_coordinator: clean
	go run -race mrcoordinator.go pg-*.txt

run_worker: clean build
	go run -race mrworker.go wc.so	
