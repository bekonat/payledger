package migrations

import "embed"

//go:embed ledger/*.sql
var Ledger embed.FS

//go:embed notifier/*.sql
var Notifier embed.FS
