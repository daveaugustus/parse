package main

import (
	"archive/zip"
	"bytes"
	"context"
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

// / UploadFile Takes the stream of data to upload a file
func (s *MigrationServer) UploadFile(stream service.MigrationDataService_UploadFileServer) error {
	log.Info("Starting the with the request to upload file")
	req, err := stream.Recv()
	serverId := req.ServerId
	fileName := req.GetMeta().GetName()
	ctx := context.Background()
	migrationId, err := createMigrationId()
	if err != nil {
		log.WithError(err).Error("Unable to create migration id")
		StreamErr(err, ctx, stream, s, migrationId, serverId, "Unable to create migration id")
		return err
	}
	log.Info("Starting with migration phase with the upload file for migration id: ", migrationId)
	_, err = s.service.Migration.StartMigration(ctx, migrationId, serverId)
	if err != nil {
		log.Errorf("Unable to insert the migration status Start Migration for  migration id : %s", migrationId)
		return err
	}
	fileData := bytes.Buffer{}
	_, err = s.service.Migration.StartFileUpload(ctx, migrationId, serverId)
	if err != nil {
		log.Errorf("Unable to insert the migration status Start File upload for  migration id : %s", migrationId)
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			errMsg := fmt.Sprintf("Failed to upload file for migration id: %s, error: %v", migrationId, err)
			StreamErr(err, ctx, stream, s, migrationId, serverId, errMsg)
			return err
		}

		chunk := req.GetChunk().Data
		_, err = fileData.Write(chunk)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to upload file for migration id: %s, error: %v", migrationId, err)
			StreamErr(err, ctx, stream, s, migrationId, serverId, errMsg)
			return err
		}
	}

	folderpath, err := saveFile(migrationId, fileName, fileData)
	if err != nil {
		StreamErr(err, ctx, stream, s, migrationId, serverId, "Failed to save uploaded file")
		return err
	}
	log.Info("File successfully saved in the directory for the requested file for migration id: ", migrationId)

	res := &response.UploadFileResponse{
		MigrationId: migrationId,
		Success:     true,
	}
	_, _ = s.service.Migration.CompleteFileUpload(ctx, migrationId, serverId, 0, 0, 0)
	log.Info("File successfully uploaded in the directory for the requested file for migration id: ", migrationId)
	err = stream.SendAndClose(res)
	if err != nil {
		handleErrorForUploadFileAndMigration(err, migrationId, serverId, s, ctx)
		log.Errorf("Failed to send the response for migration id %s : %s", migrationId, err.Error())
		return err
	}

	pipelineResult := pipeline_model.Result{Meta: pipeline_model.Meta{ZipFile: folderpath, MigrationID: migrationId, ServerID: serverId}}
	go s.phaseOnePipeline.Run(pipelineResult, s.service)
	return nil
}

func StreamErr(err error, ctx context.Context, stream service.MigrationDataService_UploadFileServer, migServer *MigrationServer, migrationId, serverId, errMsg string) {
	log.Errorf(errMsg)
	res := handleErrorForUploadFileAndMigration(err, migrationId, serverId, migServer, ctx)
	errStream := stream.SendAndClose(res)
	if errStream != nil {
		log.Errorf(errMsg)
	}
}
