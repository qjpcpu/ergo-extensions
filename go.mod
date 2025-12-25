module github.com/qjpcpu/ergo-extensions

go 1.24

replace ergo.services/registrar/zk => github.com/qjpcpu/registrar/zk v0.0.0-20251225174525-8efa82c406c2

require (
	ergo.services/ergo v1.999.310
	ergo.services/registrar/zk v0.0.0-00010101000000-000000000000
	github.com/buraksezer/consistent v0.10.0
	github.com/cespare/xxhash v1.1.0
)

require github.com/qjpcpu/zk v0.0.0-20251119061628-055c093a17d5 // indirect
