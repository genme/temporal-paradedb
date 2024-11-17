package visibility

import (
	"fmt"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

func convertCustomConfigToSQL(cfg config.CustomDatastoreConfig) (*config.SQL, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("custom datastore name is required")
	}

	sql := &config.SQL{}

	opts := cfg.Options
	var ok bool

	// Required string fields
	if sql.PluginName, ok = opts["pluginName"].(string); !ok {
		return nil, fmt.Errorf("pluginName is required and must be string")
	}
	if sql.DatabaseName, ok = opts["databaseName"].(string); !ok {
		return nil, fmt.Errorf("databaseName is required and must be string")
	}
	if sql.ConnectAddr, ok = opts["connectAddr"].(string); !ok {
		return nil, fmt.Errorf("connectAddr is required and must be string")
	}
	if sql.ConnectProtocol, ok = opts["connectProtocol"].(string); !ok {
		return nil, fmt.Errorf("connectProtocol is required and must be string")
	}

	// Optional string fields
	sql.User, _ = opts["user"].(string)
	sql.Password, _ = opts["password"].(string)

	// Optional numeric fields
	if v, ok := opts["maxConns"].(float64); ok {
		sql.MaxConns = int(v)
	}
	if v, ok := opts["maxIdleConns"].(float64); ok {
		sql.MaxIdleConns = int(v)
	}
	if v, ok := opts["taskScanPartitions"].(float64); ok {
		sql.TaskScanPartitions = int(v)
	}

	// Connect attributes
	if attrs, ok := opts["connectAttributes"].(map[string]any); ok {
		sql.ConnectAttributes = make(map[string]string)
		for k, v := range attrs {
			if str, ok := v.(string); ok {
				sql.ConnectAttributes[k] = str
			}
		}
	}

	if tlsData, ok := opts["tls"].(map[string]any); ok {
		tls := &auth.TLS{}
		if enabled, ok := tlsData["enabled"].(bool); ok {
			tls.Enabled = enabled
		}
		if certFile, ok := tlsData["certFile"].(string); ok {
			tls.CertFile = certFile
		}
		if keyFile, ok := tlsData["keyFile"].(string); ok {
			tls.KeyFile = keyFile
		}
		if caFile, ok := tlsData["caFile"].(string); ok {
			tls.CaFile = caFile
		}
		if enableHostVerification, ok := tlsData["enableHostVerification"].(bool); ok {
			tls.EnableHostVerification = enableHostVerification
		}
		if serverName, ok := tlsData["serverName"].(string); ok {
			tls.ServerName = serverName
		}
		sql.TLS = tls
	}

	return sql, nil
}
