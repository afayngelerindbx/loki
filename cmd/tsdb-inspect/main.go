package main

import (
	"github.com/grafana/loki/pkg/storage/stores/tsdb"
	"flag"
	"log"
	"context"
	"github.com/grafana/loki/clients/pkg/logentry/logql"
	"github.com/prometheus/common/model"
	"github.com/grafana/loki/pkg/logproto"
	"time"
	"fmt"
)

var (
	indexPath = flag.String("index-path", "", "path to index file to read")
	matchersStr = flag.String("matchers", "", "matchers to query")
	to = flag.String("to", "", "End Time RFC339 2006-01-02T15:04:05Z")
	from = flag.String("from", "", "End Time RFC339 2006-01-02T15:04:05Z")
)
func main() {
	flag.Parse()
	index, _, err := tsdb.NewTSDBIndexFromFile(*indexPath, true)
	if err != nil {
		log.Fatalf("Unable to open %s, err: %v", *indexPath, err)
	}

	matchers, err := logql.ParseMatchers(*matchersStr)
	if err != nil {
		log.Fatalf("Unable to parse %s, err: %v", *matchersStr, err)
	}

	chunkRefs, err := index.GetChunkRefs(
		context.Background(), "loki",
		model.TimeFromUnixNano(mustParse(*from).UnixNano()),
		model.TimeFromUnixNano(mustParse(*to).UnixNano()),
		nil,
		nil,
		matchers...,
	)
	if err != nil {
		log.Fatalf("Unable to get chunks: %v", *matchersStr, err)
	}

	for _, ref := range chunkRefs {
		protoRef := logproto.ChunkRef{
			Fingerprint: uint64(ref.Fingerprint),
			UserID:      ref.User,
			From:        ref.Start,
			Through:     ref.End,
			Checksum:    ref.Checksum,

		}

		log.Printf("ChunkRef: %s", newerExternalKey(protoRef))
	}



}


// v12+
func newerExternalKey(ref logproto.ChunkRef) string {
	return fmt.Sprintf("%s/%x/%x:%x:%x", ref.UserID, ref.Fingerprint, int64(ref.From), int64(ref.Through), ref.Checksum)
}


func mustParse(t string) time.Time {
	ret, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		log.Fatalf("Unable to parse time %v", err)
	}

	return ret
}

