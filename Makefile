default: infinitive

bindata_assetfs.go:
	go-bindata-assetfs assets/... && mv bindata.go bindata_assetfs.go

infinitive: cache.go conversions.go dispatcher.go frame.go infinitive.go protocol.go tables.go webserver.go bindata_assetfs.go
	go build infinitive
