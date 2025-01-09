[![Tests](https://github.com/rlshukhov/storage/actions/workflows/tests.yml/badge.svg)](https://github.com/rlshukhov/storage/actions/workflows/tests.yml)

# Storage
Golang storage providers

## Install 

```shell
go get github.com/rlshukhov/storage
```

## Example

```go
package main

import (
	"fmt"
	"github.com/rlshukhov/nullable"
	"github.com/rlshukhov/storage"
	"github.com/rlshukhov/storage/file"
)

type User struct {
	ID   uint64 `yaml:"id"`
	Name string `yaml:"name"`
}

func main() {
	db, err := storage.GetKeyValueProviderFromConfig[uint64, User](storage.KeyValueConfig{
		File: nullable.FromValue(file.Config{
			Path: "./users.yaml",
		}),
	})
	if err != nil {
		panic(err)
	}

	err = db.Setup()
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Shutdown()
		if err != nil {
			panic(err)
		}
	}()

	users := []User{
		{ID: 0, Name: "John"},
		{ID: 1, Name: "Paul"},
	}
	for _, user := range users {
		err := db.Store(user.ID, user)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(db.Get(1))
}
```

```shell
rlshukhov@MacBook-Pro-Lane main % go run main.go
{1 Paul} <nil>
rlshukhov@MacBook-Pro-Lane main % cat users.yaml
data:
    0:
        id: 0
        name: John
    1:
        id: 1
        name: Paul
```
