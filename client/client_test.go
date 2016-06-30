package client

import (
	//"fmt"
	//"os"
	"testing"
	"fdfs_client/config"
	"os"
	"fmt"
)

var (
	uploadResponse *UploadFileResponse
)

func TestParserFdfsConfig(t *testing.T) {
	//fc := &FdfsConfigParser{}
	//c, err := fc.Read("client.conf")
	c, err := config.ReadDefault("../client.conf")
	if err != nil {
        t.Error(err)
		return
	}
	if err != nil {
		t.Error(err)
		return
	}
	v, _ := c.String("DEFAULT", "base_path")
	t.Log(v)
}
func TestNewFdfsClientByTracker(t *testing.T) {
	tracker := &TrackerCFG{
		[]string{"210.51.190.207"},
		22122,
	}
	_, err := NewFdfsClientByTracker(tracker)
	if err != nil {
		t.Error(err)
	}
}

func TestUploadByFilename(t *testing.T) {
    trackerConf = "../client.conf"
	fdfsClient, err := NewFdfsClient()
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("../client.conf")
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
}

func TestUploadByBuffer(t *testing.T) {
	trackerConf = "../client.conf"
	fdfsClient, err := NewFdfsClient()
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	file, err := os.Open("../testfile") // For read access.
	if err != nil {
		t.Fatal(err)
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}
	fileBuffer := make([]byte, fileSize)
	_, err = file.Read(fileBuffer)
	if err != nil {
		t.Fatal(err)
	}

	uploadResponse, err = fdfsClient.UploadByBuffer(fileBuffer, "txt")
	if err != nil {
		t.Errorf("TestUploadByBuffer error %s", err.Error())
	}

	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
}

//func TestUploadSlaveByFilename(t *testing.T) {
//	fdfsClient, err := NewFdfsClient("client.conf")
//	if err != nil {
//		t.Errorf("New FdfsClient error %s", err.Error())
//		return
//	}
//
//	uploadResponse, err = fdfsClient.UploadByFilename("client.conf")
//	if err != nil {
//		t.Errorf("UploadByfilename error %s", err.Error())
//	}
//	t.Log(uploadResponse.GroupName)
//	t.Log(uploadResponse.RemoteFileId)
//
//	masterFileId := uploadResponse.RemoteFileId
//	uploadResponse, err = fdfsClient.UploadSlaveByFilename("testfile", masterFileId, "_test")
//	if err != nil {
//		t.Errorf("UploadByfilename error %s", err.Error())
//	}
//	t.Log(uploadResponse.GroupName)
//	t.Log(uploadResponse.RemoteFileId)
//
//	fdfsClient.DeleteFile(masterFileId)
//	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
//}

func TestDownloadToFile(t *testing.T) {
	trackerConf = "../client.conf"
	fdfsClient, err := NewFdfsClient()
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("../client.conf")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	var (
		downloadResponse *DownloadFileResponse
		localFilename    string = "../download.txt"
	)
	downloadResponse, err = fdfsClient.DownloadToFile(localFilename, uploadResponse.RemoteFileId, 0, 0)
	if err != nil {
		t.Errorf("DownloadToFile error %s", err.Error())
	}
	t.Log(downloadResponse.DownloadSize)
	t.Log(downloadResponse.RemoteFileId)
}

func TestDownloadToBuffer(t *testing.T) {
	trackerConf = "../client.conf"
	fdfsClient, err := NewFdfsClient()
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("../client.conf")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	var (
		downloadResponse *DownloadFileResponse
	)
	downloadResponse, err = fdfsClient.DownloadToBuffer(uploadResponse.RemoteFileId, 0, 0)
	if err != nil {
		t.Errorf("DownloadToBuffer error %s", err.Error())
	}
	t.Log(downloadResponse.DownloadSize)
	t.Log(downloadResponse.RemoteFileId)
}

func BenchmarkUploadByBuffer(b *testing.B) {
	trackerConf = "../client.conf"
	fdfsClient, err := NewFdfsClient()
	if err != nil {
		fmt.Errorf("New FdfsClient error %s", err.Error())
		return
	}
	file, err := os.Open("../testfile") // For read access.
	if err != nil {
		fmt.Errorf("%s", err.Error())
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}
	fileBuffer := make([]byte, fileSize)
	_, err = file.Read(fileBuffer)
	if err != nil {
		fmt.Errorf("%s", err.Error())
	}

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		uploadResponse, err = fdfsClient.UploadByBuffer(fileBuffer, "txt")
		if err != nil {
			fmt.Errorf("TestUploadByBuffer error %s", err.Error())
		}

		fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	}
}

func BenchmarkUploadByFilename(b *testing.B) {
	trackerConf = "../client.conf"
	fdfsClient, err := NewFdfsClient()
	if err != nil {
		fmt.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		uploadResponse, err = fdfsClient.UploadByFilename("../client.conf")
		if err != nil {
			fmt.Errorf("UploadByfilename error %s", err.Error())
		}
		err = fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
		if err != nil {
			fmt.Errorf("DeleteFile error %s", err.Error())
		}
	}
}

func BenchmarkDownloadToFile(b *testing.B) {
	trackerConf = "../client.conf"
	fdfsClient, err := NewFdfsClient()
	if err != nil {
		fmt.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("../client.conf")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		fmt.Errorf("UploadByfilename error %s", err.Error())
	}
	b.StopTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var (
			localFilename string = "../download.txt"
		)
		_, err = fdfsClient.DownloadToFile(localFilename, uploadResponse.RemoteFileId, 0, 0)
		if err != nil {
			fmt.Errorf("DownloadToFile error %s", err.Error())
		}

		// fmt.Println(downloadResponse.RemoteFileId)
	}
}
