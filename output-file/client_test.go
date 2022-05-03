package outputfile

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/myuon/ubiquitous-adventure/gallon"
	inputfile "github.com/myuon/ubiquitous-adventure/input-file"
	"github.com/myuon/ubiquitous-adventure/testdata"
	"github.com/stretchr/testify/assert"
)

func deleteFile(path string) error {
	return os.Remove(path)
}

func md5FromFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func mustMd5FromFile(path string) string {
	s, err := md5FromFile(path)
	if err != nil {
		panic(err)
	}

	return s
}

func TestJsonFile(t *testing.T) {
	outFile := fmt.Sprintf("file-%v.jsonl", gofakeit.LetterN(10))
	defer func() {
		if err := deleteFile(outFile); err != nil {
			panic(err)
		}
	}()

	g := gallon.NewGallon(
		inputfile.NewInputFileClient(inputfile.InputFileClientConfig{
			FilePath:   "../testdata/users-XUOzOdiCQC.jsonl",
			FileFormat: inputfile.Jsonl,
			Decoder: func(in []byte) (gallon.Record, error) {
				user := &testdata.User{}
				if err := json.Unmarshal([]byte(in), &user); err != nil {
					return nil, err
				}

				return gallon.Record{
					user.Id,
					user.Name,
					user.Age,
					user.CreatedAt,
				}, nil
			},
		}),
		NewOutputFileClient(OutputFileClientConfig{
			FilePath:    outFile,
			FileFormat:  Jsonl,
			Compression: "",
			Encoder: func(r gallon.Record) ([]byte, error) {
				user := testdata.User{
					Id:        r[0].(string),
					Name:      r[1].(string),
					Age:       r[2].(int),
					CreatedAt: r[3].(int64),
				}
				bs, err := json.Marshal(&user)
				if err != nil {
					return nil, err
				}

				return bs, nil
			},
		}),
	)
	assert.NoError(t, g.Run())

	type Case struct {
		command string
		want    string
	}

	cases := []Case{
		{
			command: fmt.Sprintf("wc -l %v | awk '{print $1}'", outFile),
			want:    "10000\n",
		},
		{
			command: fmt.Sprintf("cat %v | head -n 2", outFile),
			want: `{"id":"bd79f1ff-64d9-4f51-8e12-889f460fa0e0","name":"Isadore Volkman","age":94,"createdAt":-960794972}
{"id":"60995688-d496-4a76-873d-4b8f6dabccb4","name":"Sven West","age":76,"createdAt":696948764}
`,
		},
		{
			command: fmt.Sprintf("cat %v | tail -n 2", outFile),
			want: `{"id":"00180066-94fe-46e0-8e8c-7a9ef3e27a00","name":"Jordane Stokes","age":45,"createdAt":-820660725}
{"id":"b9264528-85cb-4db8-95e2-4616dc597561","name":"Louvenia Buckridge","age":65,"createdAt":450030380}
`,
		},
	}

	for _, tt := range cases {
		out, err := exec.Command("sh", "-c", tt.command).Output()
		assert.NoError(t, err)
		assert.Equal(t, tt.want, string(out))
	}
}

func TestJsonFileWithGzipOutput(t *testing.T) {
	outFile := fmt.Sprintf("file-%v.jsonl.gz", gofakeit.LetterN(10))
	defer func() {
		if err := deleteFile(outFile); err != nil {
			panic(err)
		}
	}()

	g := gallon.NewGallon(
		inputfile.NewInputFileClient(inputfile.InputFileClientConfig{
			FilePath:   "../testdata/users-XUOzOdiCQC.jsonl",
			FileFormat: inputfile.Jsonl,
			Decoder: func(in []byte) (gallon.Record, error) {
				user := &testdata.User{}
				if err := json.Unmarshal([]byte(in), &user); err != nil {
					return nil, err
				}

				return gallon.Record{
					user.Id,
					user.Name,
					user.Age,
					user.CreatedAt,
				}, nil
			},
		}),
		NewOutputFileClient(OutputFileClientConfig{
			FilePath:    outFile,
			FileFormat:  Jsonl,
			Compression: Gzip,
			Encoder: func(r gallon.Record) ([]byte, error) {
				user := testdata.User{
					Id:        r[0].(string),
					Name:      r[1].(string),
					Age:       r[2].(int),
					CreatedAt: r[3].(int64),
				}
				bs, err := json.Marshal(&user)
				if err != nil {
					return nil, err
				}

				return bs, nil
			},
		}),
	)
	assert.NoError(t, g.Run())
	assert.Equal(
		t,
		mustMd5FromFile("../testdata/users-BOWVRZukWl.jsonl.gz"),
		mustMd5FromFile(outFile),
	)
}
