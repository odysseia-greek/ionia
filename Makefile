PROTO_DIRS := diodoros thoukydides
MODULE_DIRS := artafernes diodoros hekataios herakleitos herodotos thoukydides
SERVICE_DIRS := diodoros hekataios herakleitos herodotos thoukydides

.PHONY: generate
generate:
	@for dir in $(PROTO_DIRS); do \
		buf generate --template $$dir/buf.gen.yaml $$dir; \
	done

.PHONY: tidy
tidy:
	@for dir in $(MODULE_DIRS); do \
		(cd $$dir && go mod tidy && go fmt ./...); \
	done

.PHONY: test
test:
	@for dir in $(SERVICE_DIRS); do \
		(cd $$dir && go test ./...); \
	done

.PHONY: test-system
test-system:
	@cd artafernes && go test ./peira -v

.PHONY: build
build:
	@for dir in $(MODULE_DIRS); do \
		(cd $$dir && go build ./...); \
	done
