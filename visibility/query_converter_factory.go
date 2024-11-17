// The MIT License
//
// Copyright (c) 2024 vivaneiona
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package visibility

import (
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/searchattribute"
)

// Converter TODO refactor — simplify design and decouple everything etc
type Converter interface {
	PluginQueryConverter
	BuildCountStmt() (*sqlplugin.VisibilitySelectFilter, error)
	BuildSelectStmt(
		pageSize int,
		nextPageToken []byte,
	) (*sqlplugin.VisibilitySelectFilter, error)
}

type QueryConverterFactory interface {
	NewQueryConverter(
		pluginName string,
		namespaceName namespace.Name,
		namespaceID namespace.ID,
		saTypeMap searchattribute.NameTypeMap,
		saMapper searchattribute.Mapper,
		queryString string,
	) Converter
}
