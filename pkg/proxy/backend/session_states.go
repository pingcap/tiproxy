package backend

type SessionState struct {
	sessionStates string
	sessionToken  string
}

type SessionToken struct {
	Username string `json:"username"`
}
