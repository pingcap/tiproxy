package auth

// AuthInfo the user information that is stored temporarily in the proxy.
type AuthInfo struct {
	// user information obtained during authentication
	Username    string
	AuthPlugin  string
	AuthString  []byte // password that sent from the client
	BackendSalt []byte // backend salt used to encrypt password
	Token       []byte // or password

	DefaultDB string
}
