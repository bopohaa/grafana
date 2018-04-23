package sqlstore

import (
	"github.com/bopohaa/grafana/pkg/bus"
	m "github.com/bopohaa/grafana/pkg/models"
)

func init() {
	bus.AddHandler("sql", GetDBHealthQuery)
}

func GetDBHealthQuery(query *m.GetDBHealthQuery) error {
	return x.Ping()
}
