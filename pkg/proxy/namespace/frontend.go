package namespace

type SQLInfo struct {
	SQL string
}

type FrontendNamespace struct {
	allowedDBs   []string
	allowedDBSet map[string]struct{}
	usernames    []string
	sqlBlacklist map[uint32]SQLInfo
	sqlWhitelist map[uint32]SQLInfo
}

func (n *FrontendNamespace) IsDatabaseAllowed(db string) bool {
	_, ok := n.allowedDBSet[db]
	return ok
}

func (n *FrontendNamespace) ListDatabases() []string {
	ret := make([]string, len(n.allowedDBs))
	copy(ret, n.allowedDBs)
	return ret
}

func (n *FrontendNamespace) IsDeniedSQL(sqlFeature uint32) bool {
	_, ok := n.sqlBlacklist[sqlFeature]
	return ok
}

func (n *FrontendNamespace) IsAllowedSQL(sqlFeature uint32) bool {
	_, ok := n.sqlWhitelist[sqlFeature]
	return ok
}
