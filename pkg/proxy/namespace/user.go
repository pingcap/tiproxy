package namespace

import (
	"fmt"

	"github.com/djshow832/weir/pkg/config"
	"github.com/pingcap/errors"
)

type UserNamespaceMapper struct {
	userToNamespace map[string]string
}

func CreateUserNamespaceMapper(namespaces []*config.Namespace) (*UserNamespaceMapper, error) {
	mapper := make(map[string]string)
	for _, ns := range namespaces {
		frontendNamespace := ns.Frontend
		for _, user := range frontendNamespace.Usernames {
			originNamespace, ok := mapper[user]
			if ok {
				return nil, errors.WithMessage(ErrDuplicatedUser,
					fmt.Sprintf("user: %s, namespace: %s, %s", user, originNamespace, ns.Namespace))
			}
			mapper[user] = ns.Namespace
		}
	}

	ret := &UserNamespaceMapper{userToNamespace: mapper}
	return ret, nil
}

func (u *UserNamespaceMapper) GetUserNamespace(username string) (string, bool) {
	if username == "" {
		for _, ns := range u.userToNamespace {
			return ns, true
		}
	}

	ns, ok := u.userToNamespace[username]
	return ns, ok
}

func (u *UserNamespaceMapper) Clone() *UserNamespaceMapper {
	ret := make(map[string]string)
	for k, v := range u.userToNamespace {
		ret[k] = v
	}
	return &UserNamespaceMapper{userToNamespace: ret}
}

func (u *UserNamespaceMapper) RemoveNamespaceUsers(ns string) {
	for k, namespace := range u.userToNamespace {
		if ns == namespace {
			delete(u.userToNamespace, k)
		}
	}
}

func (u *UserNamespaceMapper) AddNamespaceUsers(ns string, cfg *config.FrontendNamespace) error {
	for _, user := range cfg.Usernames {
		if originNamespace, ok := u.userToNamespace[user]; ok {
			return errors.WithMessage(ErrDuplicatedUser, fmt.Sprintf("namespace: %s", originNamespace))
		}
		u.userToNamespace[user] = ns
	}
	return nil
}
