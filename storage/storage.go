package storage

import "time"

type User struct {
	ID                  string
	ServerID            string
	InfraServerUsername string
	CredentialID        string
	Connector           string
	Email               string
	DisplayName         string
	FirstName           string
	LastName            string
	MiddleName          string
	AutomateUserID      string
	IsServerAdmin       bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
}
