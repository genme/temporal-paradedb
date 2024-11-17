package visibility

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type SqlCustomVisibilityStore struct {
	converterFactory QueryConverterFactory
}

func CustomSqlVisibilityFactory(qc QueryConverterFactory) SqlCustomVisibilityStore {
	return SqlCustomVisibilityStore{converterFactory: qc}
}

func (c SqlCustomVisibilityStore) NewVisibilityStore(
	cfg config.CustomDatastoreConfig,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	_ namespace.Registry,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (store.VisibilityStore, error) {
	sql, err := convertCustomConfigToSQL(cfg)
	if err != nil {
		return nil, err
	}

	return NewSQLVisibilityStore(
		*sql,
		r,
		saProvider,
		saMapperProvider,
		logger,
		metricsHandler,
		c.converterFactory,
	)
}
