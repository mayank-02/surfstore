.PHONY: install
install:
	rm -rf bin
	GOBIN=$(PWD)/bin go install ./...

.PHONY: run-blockstore
run-blockstore:
	go run cmd/server/main.go -s block -p 8081 -l

.PHONY: run-raft
run-raft:
	go run cmd/raft_server/main.go -f example_config.json -i $(IDX)

.PHONY: test
test:
	rm -rf ./test/_bin
	GOBIN=$(PWD)/test/_bin go get github.com/mattn/go-sqlite3
	GOBIN=$(PWD)/test/_bin go install ./...
	go test -v ./test/...

.PHONY: specific-test
specific-test:
	rm -rf ./test/_bin
	GOBIN=$(PWD)/test/_bin go get github.com/mattn/go-sqlite3
	GOBIN=$(PWD)/test/_bin go install ./...
	go test -v -run $(TEST_REGEX) -count=1 ./test/...

.PHONY: clean
clean:
	rm -rf bin/ test/_bin
