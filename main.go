package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"parse_users/pipeline"
	"parse_users/storage"

	log "github.com/sirupsen/logrus"
)

func main() {
	res := pipeline.Result{
		Meta: pipeline.Meta{
			ZipFile: "/home/dave/Downloads/backup.zip",
		},
	}

	resut, err := Unzip(res)
	if err != nil {
		log.Error(err)
	}
	fmt.Println(resut.Meta.UnzipFolder)
}

func Unzip(result pipeline.Result) (pipeline.Result, error) {
	var fpath string

	reader, err := zip.OpenReader("/home/dave/Downloads/backup.zip")
	if err != nil {
		log.Errorf("cannot open reader migration id: %s, %s", result.Meta.MigrationID, err.Error())
		return result, err
	}

	for _, file := range reader.File {
		fpath = filepath.Join(filepath.Dir(result.Meta.ZipFile), file.Name)

		if file.FileInfo().IsDir() {
			err = os.MkdirAll(fpath, os.ModePerm)
			if err != nil {
				log.Errorf("cannot create dir for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			}
			continue
		}

		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			log.Errorf("cannot create directory for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			return result, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			log.Errorf("cannot create a file for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			return result, err
		}

		readClose, err := file.Open()
		if err != nil {
			log.Errorf("cannot open file for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			return result, err
		}

		_, err = io.Copy(outFile, readClose)
		if err != nil {
			log.Errorf("cannot copy file for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			return result, err
		}
		_ = outFile.Close()
		_ = readClose.Close()
	}

	result.Meta.UnzipFolder = filepath.Dir(fpath)
	_ = reader.Close()

	return result, nil
}

func TraverseFiles(file *zip.File, result pipeline.Result) (pipeline.Result, error) {
	var fpath string
	var err error

	// Creating the files in the target directory
	if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
		log.Errorf("cannot create directory for migration id: %s, %s", result.Meta.MigrationID, err.Error())
		return result, err
	}

	// The created file will be stored in
	// outFile with permissions to write &/or truncate
	outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
	if err != nil {
		log.Errorf("cannot create a file for migration id: %s, %s", result.Meta.MigrationID, err.Error())
		return result, err
	}

	readClose, err := file.Open()
	if err != nil {
		log.Errorf("cannot open file for migration id: %s, %s", result.Meta.MigrationID, err.Error())
		return result, err
	}

	_, err = io.Copy(outFile, readClose)
	if err != nil {
		log.Errorf("cannot copy file for migration id: %s, %s", result.Meta.MigrationID, err.Error())
		return result, err
	}
	_ = outFile.Close()
	_ = readClose.Close()

	return result, nil
}

func Validate(result pipeline.Result) {
	// Find under unzip folder where org folder exists
	if err := filepath.Walk(result.Meta.UnzipFolder, func(path string, f os.FileInfo, err error) error {
		fmt.Println(path, filepath.Dir(path))
		if filepath.Base(path) == "organizations" {
			result.Meta.UnzipFolder = filepath.Dir(path)
			fmt.Println("Found Organization")
		}
		time.Sleep(time.Second)
		return nil
	}); err != nil {
		fmt.Errorf("error: %s\n", err.Error())
	}

}

func GetUsersForBackup(result pipeline.Result) (pipeline.Result, error) {

	file := path.Join(result.Meta.UnzipFolder, "key_dump.json")

	var keyDumps []pipeline.KeyDump

	f, err := os.Open(file)
	if err != nil {
		return result, err
	}
	err = json.NewDecoder(f).Decode(&keyDumps)
	if err != nil {
		return result, err
	}

	serverUsers := keyDumpTOUser(keyDumps)
	automateUsers := []storage.User{
		{
			InfraServerUsername: "demoname12",
			Email:               "demoname@dummy.com",
		},

		{
			InfraServerUsername: "test-new-user",
			Email:               "test-new@email.com",
		},

		{
			InfraServerUsername: "other-user4",
			Email:               "root@localhost",
		},

		{
			InfraServerUsername: "davetweetlive",
			Email:               "abc",
		},
		{
			InfraServerUsername: "kallol",
			Email:               "kallol.roy@progress.com",
		},
		{
			InfraServerUsername: "automate123456",
			Email:               "abc",
		},
	}
	if err != nil {
		return result, err
	}
	var mappedUsers []pipeline.User

	mappedUsers = append(mappedUsers, deleteUser(serverUsers, automateUsers)...)
	mappedUsers = append(mappedUsers, insertUpdateSkipUser(serverUsers, automateUsers)...)

	result.ParsedResult.Users = mappedUsers
	return result, nil
}

// Clean serialized_object and Polulate Users struct
func keyDumpTOUser(keyDump []pipeline.KeyDump) []pipeline.User {
	var users []pipeline.User
	for _, kd := range keyDump {
		sec := map[string]string{}
		if err := json.Unmarshal([]byte(kd.SerializedObject), &sec); err != nil {
			log.Errorf("failed to pasre user's first, middle and last name: %s", err.Error())
		}
		user := pipeline.User{
			Username:    kd.Username,
			Email:       kd.Email,
			DisplayName: sec["display_name"],
			FirstName:   sec["first_name"],
			LastName:    sec["last_name"],
			MiddleName:  sec["middle_name"],
		}
		users = append(users, user)
	}
	return users
}

func automateMap(automateUser []storage.User) map[string]storage.User {
	autoMap := map[string]storage.User{}
	for _, auser := range automateUser {
		autoMap[auser.InfraServerUsername] = auser
	}
	return autoMap
}

func serverMap(server []pipeline.User) map[string]pipeline.User {
	autoMap := map[string]pipeline.User{}
	for _, auser := range server {
		autoMap[auser.Username] = auser
	}
	return autoMap
}
func insertUpdateSkipUser(serverUser []pipeline.User, automateUser []storage.User) []pipeline.User {
	var parsedUsers []pipeline.User
	autoMap := automateMap(automateUser)
	for _, sUser := range serverUser {

		if autoMap[sUser.Username].InfraServerUsername != "" {
			emptyVal := pipeline.User{}
			returnedVal := skipOrUpdate(autoMap, sUser)
			if returnedVal != emptyVal {
				parsedUsers = append(parsedUsers, returnedVal)
			}
			// if autoMap[sUser.Username].InfraServerUsername == sUser.Username {
			// 	if autoMap[sUser.Username].InfraServerUsername == sUser.Username && autoMap[sUser.Username].Email == sUser.Email {
			// 		sUser.ActionOps = pipeline.Skip
			// 		parsedUsers = append(parsedUsers, sUser)

			// 	} else {
			// 		sUser.ActionOps = pipeline.Update
			// 		parsedUsers = append(parsedUsers, sUser)
			// 	}
			// }
			fmt.Println("UPDATE, SKIP", sUser.Username)
		} else {
			if sUser.Username == "pivotal" {
				sUser.ActionOps = pipeline.Skip
				parsedUsers = append(parsedUsers, sUser)
			} else {
				sUser.ActionOps = pipeline.Insert
				parsedUsers = append(parsedUsers, sUser)
				fmt.Println("INSERT", sUser.Username)
			}

		}

	}

	return parsedUsers
}

func skipOrUpdate(autoMap map[string]storage.User, sUser pipeline.User) pipeline.User {
	if autoMap[sUser.Username].InfraServerUsername == sUser.Username {
		if autoMap[sUser.Username].InfraServerUsername == sUser.Username && autoMap[sUser.Username].Email == sUser.Email {
			sUser.ActionOps = pipeline.Skip
			// parsedUsers = append(parsedUsers, sUser)
			return sUser

		} else {
			sUser.ActionOps = pipeline.Update
			// parsedUsers = append(parsedUsers, sUser)
			return sUser
		}
	}
	return pipeline.User{}
}

func deleteUser(serverUser []pipeline.User, automateUser []storage.User) []pipeline.User {
	var parsedUsers []pipeline.User
	autoMap := serverMap(serverUser)
	for _, aUser := range automateUser {
		if autoMap[aUser.InfraServerUsername].Username == "" {
			parsedUsers = append(parsedUsers, pipeline.User{
				Username:  aUser.InfraServerUsername,
				ActionOps: pipeline.Delete,
			})
		}

	}

	return parsedUsers
}
