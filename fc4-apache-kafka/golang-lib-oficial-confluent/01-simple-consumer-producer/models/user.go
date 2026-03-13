package models

// UserEvent representa o dado que será enviado ao Kafka
type UserEvent struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}
