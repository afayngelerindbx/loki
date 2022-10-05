package main

import (
	"flag"
	"log"
	"context"
	"gopkg.in/yaml.v2"
	"github.com/grafana/loki/pkg/storage/stores/indexshipper/compactor/retention"
	"github.com/grafana/loki/pkg/storage/stores/shipper/index/compactor"
	"github.com/grafana/loki/pkg/storage/config"
	shipper_util "github.com/grafana/loki/pkg/storage/stores/shipper/util"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var (
	indexPath = flag.String("index-path", "", "path to index file to read")
	// Hardcode a periodconfig for convenience as the boltdb iterator needs one
	// NB: must match the index file you're reading from
	periodConfig = func() config.PeriodConfig {
		input := `
from: "2022-01-01"
index:
  period: 24h
  prefix: loki_index_
object_store: gcs
schema: v13
store: boltdb-shipper
`
		var cfg config.PeriodConfig
		if err := yaml.Unmarshal([]byte(input), &cfg); err != nil {
			panic(err)
		}
		return cfg
	}()

)

func main() {
	flag.Parse()

	db, err := shipper_util.SafeOpenBoltdbFile(*indexPath)
	if err != nil {
		panic(err)
	}
	if err = db.View(func(t *bbolt.Tx) error {
		return compactor.ForEachChunk(context.Background(), t.Bucket([]byte("loki")), periodConfig, func(entry retention.ChunkEntry) (bool, error) {
			log.Printf("labels %s, fp: %x", entry.Labels, entry.Labels.Hash())
			tmp := append(entry.Labels, labels.Label{Name: "agent_host_id", Value: ""})
			entry.Labels = labels.New(tmp...)
			log.Printf("labels %s, fp: %x", entry.Labels, entry.Labels.Hash())
			return false, nil
		})
	}) ; err != nil {
		panic(err)
	}


}
