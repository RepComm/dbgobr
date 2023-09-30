package main

import (
	"bufio"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type CmdNodeExec func(argsMap ArgsMap)

type ArgsMap map[string]string

type CmdNode struct {
	Name     string
	Data     string
	Children []CmdNode
	Exec     CmdNodeExec
}

type CmdParseState struct {
	parts      []string
	parent     *CmdNode
	args       *ArgsMap
	lowestExec *CmdNodeExec
}

func process_cmd(s *CmdParseState) bool {
	if s.args == nil {
		args := make(ArgsMap)
		s.args = &args
	}

	lowestExec := s.lowestExec

	count := 0

	found := false
	part := ""

	for _, child := range s.parent.Children {
		part = s.parts[count]
		count++

		if child.Data == "" {
			//if node doesn't specify required data

			(*s.args)[child.Name] = part
			found = true
			//record as value where node.Name is the key

		} else if child.Data == part {
			//if node specifies data and we've matched it

			if child.Exec != nil {
				lowestExec = &child.Exec
				s.lowestExec = lowestExec
			}
			if count >= len(s.parts) {

				hasOne := len(child.Children) > 0
				hasInput := hasOne && child.Children[0].Data == ""

				if !hasOne {
					found = true
					break
				}

				fmt.Println(part, "command requires more arguments than are provided")

				if hasInput {
					fmt.Print("Expected args ")
					for _, child := range child.Children {
						if child.Data == "" {
							fmt.Print("[", child.Name, "]")
						} else {
							fmt.Print(child.Data)
						}
						fmt.Print(", ")
					}
					fmt.Print("\n")
				} else {
					fmt.Print("Expected one of [ ")
					for _, child := range child.Children {
						if child.Data == "" {
							fmt.Print("[", child.Name, "]")
						} else {
							fmt.Print(child.Data)
						}
						fmt.Print(", ")
					}
					fmt.Print("]\n")
				}

				return false
			}

			ns := CmdParseState{
				parts:      s.parts[count:],
				parent:     &child,
				args:       s.args,
				lowestExec: lowestExec,
			}

			if !process_cmd(&ns) {
				return false
			}
			found = true

			s.lowestExec = ns.lowestExec
			//try to parse more, but return false if it fails

			break
			//stop looping over children as we can only go down one path
		} else {
			// return false
			found = false
			count--
			if count < 0 {
				return false
			}
		}
	}

	if !found {
		fmt.Print("Expected one of [ ")
		for _, child := range s.parent.Children {
			if child.Data == "" {
				fmt.Print("[", child.Name, "]")
			} else {
				fmt.Print(child.Data)
			}
			fmt.Print(", ")
		}
		fmt.Print("] but got \"", part, "\"\n")
	}

	return found
}

func process_str(text string, cmds *CmdNode) {
	parts := strings.Fields(text)

	s := CmdParseState{
		parts:      parts,
		parent:     cmds,
		args:       nil,
		lowestExec: nil,
	}

	if process_cmd(&s) {
		if s.lowestExec != nil {
			(*s.lowestExec)(*s.args)
		}
	}
}

func repl(cmds *CmdNode) {
	loop := true

	reader := bufio.NewReader(os.Stdin)

	for loop {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')

		process_str(text, cmds)
	}
}

func table_create(id string, desc string) {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	fname := fmt.Sprintf("%s.table", id)
	fpath := path.Join(wd, "./db/tables", fname)
	fdir := filepath.Dir(fpath)

	fmt.Println("fname", fname, "fpath", fpath, "fdir", fdir)

	err = os.MkdirAll(fdir, os.ModePerm)
	if err != nil {
		panic(err)
	}

	file, err := os.Create(fpath)
	if err != nil {
		panic(err)
	}
	// defer file.Close()

	writer := bufio.NewWriter(file)

	writer.WriteString(fmt.Sprintf("table %s\ndesc %s\n", id, desc))
	writer.Flush()
	file.Close()
}

var hash_string_v hash.Hash32 = fnv.New32()

func hash_string(content string) uint32 {
	hash_string_v.Reset()
	hash_string_v.Write([]byte(content))
	return hash_string_v.Sum32()
}

func main() {

	fmt.Println("DbGoBr", hash_string("DbGoBr"))

	cmds := CmdNode{
		Name: "root",
		Data: "",

		Children: []CmdNode{
			{
				Name: "table",
				Data: "table",

				Children: []CmdNode{
					{
						Name: "create",
						Data: "create",
						Exec: func(argsMap ArgsMap) {
							table_create(argsMap["table.name"], argsMap["table.desc"])
							fmt.Println("- Created table", argsMap["table.name"])

						},
						Children: []CmdNode{
							{
								Name: "table.name",
								Data: "",
							},
							{
								Name: "table.desc",
								Data: "",
							},
						},
					},

					{
						Name: "delete",
						Data: "delete",
						Exec: func(argsMap ArgsMap) {
							fmt.Println("- Deleted table", argsMap["table.name"])
						},
						Children: []CmdNode{
							{
								Name: "table.name",
								Data: "",
							},
						},
					},
				},
			},
			{
				Name: "clear",
				Data: "clear",
				Exec: func(argsMap ArgsMap) {
					fmt.Print("\033[H\033[2J")
				},
			},
			{
				Name: "exit",
				Data: "exit",
				Exec: func(argsMap ArgsMap) {
					os.Exit(0)
				},
			},
		},
	}

	repl(&cmds)
}
