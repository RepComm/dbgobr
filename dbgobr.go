package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
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

/** Generate the file path of a file in the working directory
 *
 */
func wd_file(fname string) string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	fpath := path.Join(wd, fname)
	return fpath
}

/** Ensure a directory exists
 * accepts a file path as well, but will clip off the file name
 */
func ensure_fdir(fpath string) {
	fdir := filepath.Dir(fpath)

	err := os.MkdirAll(fdir, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

// check if a file exists
func file_exists(fpath string) bool {
	_, err := os.Stat(fpath)
	if err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		panic(err)
	}
}

// creates if doesn't exist, but doesn't create directories
func file_open(fpath string) *os.File {
	var file *os.File
	var err error

	if !file_exists(fpath) {
		file, err = os.Create(fpath)
	} else {
		file, err = os.OpenFile(fpath, os.O_RDWR, 0644)
	}
	if err != nil {
		panic(err)
	}
	return file
}

/** WARN: You are responsible for calling file.Close()
 * Ensure a file specified by its path exists
 * including directories that may not exist
 */
func ensure_file(fpath string) *os.File {
	ensure_fdir(fpath)

	return file_open(fpath)
}

// alias
func fstr(s string, a ...any) string {
	return fmt.Sprintf(s, a...)
}

const (
	ColumnString = iota
	ColumnInt
	ColumnFloat
)

func ValueToColumnType(v any) (int, error) {
	switch v.(type) {
	case string:
		return ColumnString, nil
	case int:
		return ColumnInt, nil
	case float64:
		return ColumnFloat, nil
	default:
		return 0, fmt.Errorf("Unhandled value type %s", v)
	}
}

var intToBytesBuf *bytes.Buffer = new(bytes.Buffer)

func intToBytes(v int) []byte {
	binary.Write(intToBytesBuf, binary.LittleEndian, v)
	return intToBytesBuf.Bytes()
}
func uint32ToBytes(v uint32) []byte {
	binary.Write(intToBytesBuf, binary.LittleEndian, v)
	return intToBytesBuf.Bytes()
}

func table_create(def *TableDef) {
	dbDef.Tables[def.Id] = def
}

var hash_string_v hash.Hash32 = fnv.New32()

func hash_string(content string) uint32 {
	hash_string_v.Reset()
	hash_string_v.Write([]byte(content))
	return hash_string_v.Sum32()
}

type ColumnDef struct {
	Type string
	Id   string
}

func stringToColumnType(t string) (int, error) {
	typeValue := reflect.ValueOf(t)
	return ValueToColumnType(typeValue)
}

type ColumnMap map[string]*ColumnDef

type TableDef struct {
	Id      string
	Desc    string
	Columns ColumnMap
}

type TableMap map[string]*TableDef

type DbDef struct {
	Id     string
	Desc   string
	Tables TableMap
}

var dbDefFname = "./db/defs.json"
var dbDefFpath = wd_file(dbDefFname)

var dbDef *DbDef = &DbDef{}

func dbDefLoad() {
	data, err := os.ReadFile(dbDefFpath)
	if err != nil {
		fmt.Println("Error reading file", err)
		panic(err)
	}

	err = json.Unmarshal(data, dbDef)
	if err != nil {
		fmt.Println("Error parsing json", err)
		panic(err)
	}
}
func dbDefSave() {
	data, err := json.Marshal(dbDef)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(dbDefFpath, data, 0644)
	if err != nil {
		panic(err)
	}
}

func (self *TableDef) calcByteLen() int {
	result := 0
	for _, col := range self.Columns {
		switch col.Type {
		case "string32":
			result += 32
			break
		case "string256":
			result += 256
			break
		case "string1024":
			result += 1024
			break
		case "string2048":
			result += 2048
			break
		case "int":
		case "float64":
			result += 8
			break
		case "byte":
		case "boolean":
			result += 1
			break
		case "fkey":
			result += 8 + 32 //int (table id) + table name (32 char max)
			break
		}
	}
	return result
}

func find_table_def(id string) (*TableDef, error) {
	td := dbDef.Tables[id]
	if td == nil {
		return nil, fmt.Errorf("table by id %s not found, cannot insert", id)
	}
	return td, nil
}

func str_clip(s string, max int) string {
	return s[:min(len(s), max)]
}

func table_insert(argsMap ArgsMap) {
	tableName := argsMap["table.name"]

	td, err := find_table_def(tableName)
	if err != nil {
		fmt.Println(err)
		return
	}

	str := argsMap["table.data"]

	parts := strings.Split(str, ",")
	for _, part := range parts {
		kv := strings.Split(part, "=")
		if len(kv) < 2 {
			fmt.Println("Expected [key]=[value], but only found [key]=, this is invalid. table insert cancelled")
			return
		}
		k := kv[0]
		v := kv[1]

		if td.Columns == nil {
			fmt.Println("Table", tableName, "does not have any columns defined, ignoring")
			return
		}

		col := td.Columns[k]
		if col == nil {
			fmt.Println("Key: ", k, "is not present in table", tableName, "ignoring")
			continue
		}
		// fmt.Println("Key: ", k, "Value: ", v)
		argsMap[k] = v
	}

	bLen := td.calcByteLen()
	fmt.Println("Generating byte buffer w/ size", bLen, "bytes")

	rowBuffer := make([]byte, bLen)
	offset := 0

	for _, col := range td.Columns {
		v := argsMap[col.Id]
		if v == "" {
			fmt.Print("Missing key \"",
				col.Id,
				"\" of type ",
				col.Type,
				", cannot insert into table\n",
			)
			return
		}
		// fmt.Println("Preparing", col.Id, "as", col.Type)
		switch col.Type {
		case "string32":
			copy(rowBuffer[offset:], str_clip(v, 32))
			offset += 32
			break
		case "string128":
			copy(rowBuffer[offset:], str_clip(v, 128))
			offset += 128
			break
		case "string256":
			copy(rowBuffer[offset:], str_clip(v, 256))
			offset += 256
			break
		case "boolean":
			if v == "true" {
				rowBuffer[offset] = 1
			} else {
				rowBuffer[offset] = 1
			}
			offset += 1
			break
		}
	}

	fmt.Println("Inserting", str, "into table", tableName)

	fname := fstr("./db/tables/%s.table", tableName)
	fpath := wd_file(fname)
	file := ensure_file(fpath)

	seeked, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		fmt.Println("Error seeking to end of file for appending", err)
		panic(err)
	}
	fmt.Println("Writing at", seeked, "bytes offset into table file")
	_, err = file.Write(rowBuffer)

	if err != nil {
		fmt.Println("Error writing file", err)
	}

	file.Close()
}

func main() {

	if file_exists(dbDefFpath) {
		fmt.Println("Found db/defs.json, loaded")
		dbDefLoad()
	} else {
		fmt.Println("No db/defs.json, created")
		dbDef = &DbDef{
			Id:   "demo",
			Desc: "A demo database",
			Tables: TableMap{
				"users": {
					Id:   "users",
					Desc: "A user of the software",
					Columns: ColumnMap{
						"username": {
							Id:   "username",
							Type: "string32",
						},
						"verified": {
							Id:   "verified",
							Type: "boolean",
						},
					},
				},
			},
		}
		dbDefSave()
	}

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
							def := TableDef{
								Id:   argsMap["table.name"],
								Desc: argsMap["table.desc"],
							}

							table_create(&def)

							dbDefSave()

							fmt.Println("- Created table", def.Id)

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
							toRemoveName := argsMap["table.name"]

							toRemove := dbDef.Tables[toRemoveName]

							if toRemove == nil {
								fmt.Println("Didn't find table by id", toRemoveName, "cannot remove it")
							} else {
								delete(dbDef.Tables, toRemoveName)
								dbDefSave()
								fmt.Println("- Deleted table", toRemoveName)
							}

						},
						Children: []CmdNode{
							{
								Name: "table.name",
								Data: "",
							},
						},
					},

					{
						Name: "insert",
						Data: "insert",
						Exec: table_insert,
						Children: []CmdNode{
							{
								Name: "into",
								Data: "into",
								Children: []CmdNode{
									{
										Name: "table.name",
										Data: "",
									},
									{
										Name: "table.data",
										Data: "",
									},
								},
							},
						},
					},

					{
						Name: "list",
						Data: "list",
						Exec: func(argsMap ArgsMap) {
							fmt.Print("List of all tables: [")
							for key := range dbDef.Tables {
								fmt.Print(key, ", ")
							}
							fmt.Println("]")
						},
					},

					{
						Name: "inspect",
						Data: "inspect",
						Exec: func(argsMap ArgsMap) {
							tn := argsMap["table.name"]
							td, err := find_table_def(tn)
							if err != nil {
								fmt.Println(err)
								return
							}
							colCount := 0
							if td.Columns != nil {
								colCount = len(td.Columns)
							}
							fmt.Print(
								"Table ",
								td.Id,
								" has ",
								colCount,
								" columns [",
							)

							for _, col := range td.Columns {
								fmt.Print(col.Id, ", ")
							}
							fmt.Println("]")
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
