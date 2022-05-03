package testdata

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type User struct {
	Id        string `json:"id"`
	Name      string `json:"name"`
	Age       int    `json:"age"`
	CreatedAt int64  `json:"createdAt"`
}

type Users []User

func GenerateFakeUsers(n int) Users {
	users := make([]User, n)
	for i := 0; i < n; i++ {
		users[i] = User{
			Id:        gofakeit.UUID(),
			Name:      gofakeit.Name(),
			Age:       gofakeit.Number(18, 100),
			CreatedAt: gofakeit.Date().Unix(),
		}
	}

	return users
}

func (us Users) Jsonl() string {
	var lines []string
	for _, u := range us {
		bs, err := json.Marshal(&u)
		if err != nil {
			panic(err)
		}

		lines = append(lines, string(bs))
	}

	return strings.Join(lines, "\n")
}

func CreateFile(path string, content string) error {
	fs, err := os.Create(path)
	if err != nil {
		return err
	}

	if _, err := fs.WriteString(content); err != nil {
		return err
	}

	if err := fs.Close(); err != nil {
		return err
	}

	return nil
}
