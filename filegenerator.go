package main

import (
	"fmt"
	"os"

	"syreclabs.com/go/faker"
)

func FillFile() {
	file, err := os.OpenFile("file.csv", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var line string
	for i := 1; i <= 50000; i++ {
		line = createLine()
		file.WriteString(line)
	}
}

func createLine() string {
	return fmt.Sprintf(
		"%s,%s,%s\n",
		faker.Name().Name(),
		faker.Number().Between(18, 50),
		faker.Internet().Email(),
	)
}
