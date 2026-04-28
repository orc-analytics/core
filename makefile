.PHONY: all build_store remove_store refresh_store

all: .proto .datalayer
datalayer: .datalayer

build_store: .create_ssl_cert .spin_up_datalayer
start_store: .start_datalayer
stop_store: .stop_datalayer
remove_store: .remove_datalayer
redo_store: .remove_datalayer .remove_store_cache .create_ssl_cert .spin_up_datalayer
create_ssl: .create_ssl_cert
test: .test_all

# flags for stripping debugging info
LDFLAGS = -s -w

## CLI Build scripts
BINARY_NAME = orca

# disabled CGO to produce statically-linked binaries
export CGO_ENABLED = 0

.datalayer:
	sqlc vet -f sqlc.yaml
	sqlc generate -f sqlc.yaml

.stop_datalayer:
	cd local_storage && docker-compose stop

.start_datalayer:
	cd local_storage && docker-compose start

.remove_datalayer:
	cd local_storage && docker-compose down
	docker volume remove local_storage_datalayer

.spin_up_datalayer:
	@if [ ! -d "./local_storage/_datalayer" ]; then \
        sudo mkdir -p ./local_storage/_datalayer; \
				sudo chmod 777 ./local_storage/_datalayer; \
	fi
	cd local_storage && docker-compose up -d

.remove_store_cache:
	sudo rm -rf local_storage/_*

.create_ssl_cert:
	@if [ ! -d "./local_storage/_ca" ]; then \
        sudo mkdir -p ./local_storage/_ca; \
				sudo chmod 777 ./local_storage/_ca; \
	fi
	cd ./local_storage/_ca && \
		sudo openssl req -new -text -passout pass:abcd -subj /CN=localhost -out server.req -keyout privkey.pem
	cd ./local_storage/_ca && \
		sudo openssl rsa -in privkey.pem -passin pass:abcd -out server.key
	cd ./local_storage/_ca && \
		sudo openssl req -x509 -in server.req -text -key server.key -out server.crt
	sudo chown 0:70 local_storage/_ca/server.key
	sudo chmod 640 local_storage/_ca/server.key

.test_all:
	go test ./... -v

# ------------- BUILD -------------

#  build command
BUILD = go build -ldflags "$(LDFLAGS)" -o

# platform targets
build_all: linux windows mac_arm mac_intel

linux:
	GOOS=linux GOARCH=amd64 $(BUILD) ./build/$(BINARY_NAME)-amd64-linux .

windows:
	GOOS=windows GOARCH=amd64 $(BUILD) ./build/$(BINARY_NAME)-amd64-windows.exe .

mac_arm:
	GOOS=darwin GOARCH=arm64 $(BUILD) ./build/$(BINARY_NAME)-amd64-mac-arm .

mac_intel:
	GOOS=darwin GOARCH=amd64 $(BUILD) ./build/$(BINARY_NAME)-amd64-mac-intel .

cli_clean:
	rm -rf build/
