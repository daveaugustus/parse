package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"parse_users/pipeline"

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
				errF := fmt.Sprintf("cannot create dir for migration id: %s, %s", result.Meta.MigrationID, err.Error())
				log.Errorf(errF)
			}
			continue
		}

		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			errF := fmt.Sprintf("cannot create dir for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			log.Errorf(errF)
			return result, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			errF := fmt.Sprintf("cannot create dir for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			log.Errorf(errF)
			return result, err
		}

		readClose, err := file.Open()
		if err != nil {
			errF := fmt.Sprintf("cannot create dir for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			log.Errorf(errF)
			return result, err
		}

		_, err = io.Copy(outFile, readClose)
		if err != nil {
			errF := fmt.Sprintf("cannot create dir for migration id: %s, %s", result.Meta.MigrationID, err.Error())
			log.Errorf(errF)
			return result, err
		}
		_ = outFile.Close()
		_ = readClose.Close()
	}

	result.Meta.UnzipFolder = filepath.Dir(fpath)
	_ = reader.Close()

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
