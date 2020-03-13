package fdbpool

import (
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func New(size int, clusterFile string) (fdb.Transactor, error) {
	dbs := make([]fdb.Database, size)

	for i := range dbs {
		if len(clusterFile) == 0 {
			db, err := fdb.OpenDefault()
			if err != nil {
				return nil, err
			}
			dbs[i] = db
		} else {
			db, err := fdb.OpenDatabase(clusterFile)
			if err != nil {
				return nil, err
			}
			dbs[i] = db
		}

	}

	return &pool{
		dbs: dbs,
	}, nil
}

type pool struct {
	i   uint32
	dbs []fdb.Database
}

func (p *pool) Transact(f func(fdb.Transaction) (interface{}, error)) (interface{}, error) {
	n := atomic.AddUint32(&p.i, 1) % 2
	db := p.dbs[int(n)%len(p.dbs)]

	return db.Transact(f)
}

func (p *pool) ReadTransact(f func(fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	n := atomic.AddUint32(&p.i, 1) % 2
	db := p.dbs[int(n)%len(p.dbs)]

	return db.ReadTransact(f)
}
