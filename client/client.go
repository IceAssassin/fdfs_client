package client

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"fdfs_client/config"
)

var (
    trackerConf          string                     = "./config/client.conf"
    trackerCFG           *TrackerCFG                = nil
    trackerMinConn       int                        = 10
    trackerMaxConn       int                        = 150
	storageMinConn       int                        = 10
	storageMaxConn       int                        = 150

	trackerPoolChan      chan *trackerPool          = make(chan *trackerPool, 1)
	trackerPoolMap       map[string]*ConnectionPool = make(map[string]*ConnectionPool)
	fetchTrackerPoolChan chan interface{}          = make(chan interface{}, 1)
    quitTrackerLoop      chan bool

	storagePoolChan      chan *storagePool          = make(chan *storagePool, 1)
	storagePoolMap       map[string]*ConnectionPool = make(map[string]*ConnectionPool)
	fetchStoragePoolChan chan interface{}          = make(chan interface{}, 1)
	quitStorageLoop      chan bool
)

type FdfsClient struct {
	//tracker     *Tracker
	trackerPool *ConnectionPool
	timeout     int
}

type TrackerCFG struct {
	HostList []string
	Port     int
}

type trackerPool struct {
	trackerPoolKey string
	host          string
	port           int
	minConns       int
	maxConns       int
}

type storagePool struct {
	storagePoolKey string
	hosts          []string
	port           int
	minConns       int
	maxConns       int
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	go func() {
		// start a loop
		running := true
		for running {
			select {
			case spd := <-storagePoolChan:
				if sp, ok := storagePoolMap[spd.storagePoolKey]; ok && len(sp.conns) > 0  {
					fetchStoragePoolChan <- sp
				} else {
					var (
						sp  *ConnectionPool
						err error
					)
					sp, err = NewConnectionPool(spd.hosts, spd.port, spd.minConns, spd.maxConns)
					if err != nil {
						fetchStoragePoolChan <- err
					} else {
						storagePoolMap[spd.storagePoolKey] = sp
						fetchStoragePoolChan <- sp
					}
				}
			case <-quitStorageLoop:
				running = false
				break
			}
		}
	}()

	go func() {
		// start a loop
		running := true
		for running {
			select {
			case tpd := <-trackerPoolChan:
				if tp, ok := trackerPoolMap[tpd.trackerPoolKey]; ok && len(tp.conns) > 0 {
					fetchTrackerPoolChan <- tp
				} else {
					var (
						tp  *ConnectionPool
						err error
					)
					tp, err = NewConnectionPool([]string{tpd.host}, tpd.port, tpd.minConns, tpd.maxConns)
					if err != nil {
						fetchTrackerPoolChan <- err
					} else {
						trackerPoolMap[tpd.trackerPoolKey] = tp
						fetchTrackerPoolChan <- tp
					}
				}
			case <-quitTrackerLoop:
				running = false
				break
			}
		}
	}()
}

func NewFdfsClient() (*FdfsClient, error) {
	tracker, err := getTrackerConf()
	if err != nil {
		return nil, err
	}

	var trackerPool *ConnectionPool = nil
	for _, host := range tracker.HostList {
        trackerPool, err = getTrackerPool(host, tracker.Port)
		if err != nil {
			continue
		}
	}

    if err != nil {
        return nil, err
    }

	return &FdfsClient{trackerPool: trackerPool}, nil
}

func NewFdfsClientByTracker(tracker *TrackerCFG) (*FdfsClient, error) {
	var (
	    trackerPool *ConnectionPool = nil
	    err         error           = nil
	)

	for _, host := range tracker.HostList {
		trackerPool, err = getTrackerPool(host, tracker.Port)
		if err != nil {
			continue
		}
	}

	if err != nil {
		return nil, err
	}

	return &FdfsClient{trackerPool: trackerPool}, nil
}

func CloseFdfsClient() {
	quitStorageLoop <- true
	quitTrackerLoop <- true
}

func (this *FdfsClient) UploadByFilename(filename string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageUploadByFilename(tc, storeServ, filename)
}

func (this *FdfsClient) UploadByBuffer(filebuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageUploadByBuffer(tc, storeServ, filebuffer, fileExtName)
}

func (this *FdfsClient) UploadSlaveByFilename(filename, remoteFileId, prefixName string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageUploadSlaveByFilename(tc, storeServ, filename, prefixName, remoteFilename)
}

func (this *FdfsClient) UploadSlaveByBuffer(filebuffer []byte, remoteFileId, fileExtName string) (*UploadFileResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageUploadSlaveByBuffer(tc, storeServ, filebuffer, remoteFilename, fileExtName)
}

func (this *FdfsClient) UploadAppenderByFilename(filename string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageUploadAppenderByFilename(tc, storeServ, filename)
}

func (this *FdfsClient) UploadAppenderByBuffer(filebuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageUploadAppenderByBuffer(tc, storeServ, filebuffer, fileExtName)
}

func (this *FdfsClient) DeleteFile(remoteFileId string) error {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageUpdate(groupName, remoteFilename)
	if err != nil {
		return err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageDeleteFile(tc, storeServ, remoteFilename)
}

func (this *FdfsClient) DownloadToFile(localFilename string, remoteFileId string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	return store.storageDownloadToFile(tc, storeServ, localFilename, offset, downloadSize, remoteFilename)
}

func (this *FdfsClient) DownloadToBuffer(remoteFileId string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	storeServ, err := tc.trackerQueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	store := &StorageClient{storagePool}

	var fileBuffer []byte
	return store.storageDownloadToBuffer(tc, storeServ, fileBuffer, offset, downloadSize, remoteFilename)
}

func getTrackerConf() (*TrackerCFG, error) {
    if trackerCFG != nil {
    return trackerCFG, nil
    }

    return loadTrackerConf(trackerConf)
}

func loadTrackerConf(confPath string) (*TrackerCFG, error) {
	cf, err := config.ReadDefault(confPath)
	if err != nil {
		return nil, err
	}
	trackerListString, _ := cf.RawString("DEFAULT", "tracker_server")
	trackerList := strings.Split(trackerListString, ",")

	var (
		trackerIpList []string
		trackerPort   string = "22122"
	)

	for _, tr := range trackerList {
		var trackerIp string
		tr = strings.TrimSpace(tr)
		parts := strings.Split(tr, ":")
		trackerIp = parts[0]
		if len(parts) == 2 {
			trackerPort = parts[1]
		}
		if trackerIp != "" {
			trackerIpList = append(trackerIpList, trackerIp)
		}
	}
	tp, err := strconv.Atoi(trackerPort)
	tracer := &TrackerCFG{
		HostList: trackerIpList,
		Port:     tp,
	}
	return tracer, nil
}

func getTrackerPool(host string, port int) (*ConnectionPool, error) {
	var (
		trackerPoolKey string = fmt.Sprintf("%s-%d", host, port)
		result         interface{}
		err            error
		ok             bool
	)

	tpd := &trackerPool{
		trackerPoolKey: trackerPoolKey,
		host:          host,
		port:           port,
		minConns:       trackerMinConn,
		maxConns:       trackerMaxConn,
	}
	trackerPoolChan <- tpd
	select {
	case result = <-fetchTrackerPoolChan:
		var trackerPool *ConnectionPool
		if err, ok = result.(error); ok {
			return nil, err
		} else if trackerPool, ok = result.(*ConnectionPool); ok {
			return trackerPool, nil
		} else {
			return nil, errors.New("none")
		}
	}
}

func (this *FdfsClient) getStoragePool(ipAddr string, port int) (*ConnectionPool, error) {
	hosts := []string{ipAddr}
	var (
		storagePoolKey string = fmt.Sprintf("%s-%d", hosts[0], port)
		result         interface{}
		err            error
		ok             bool
	)

	spd := &storagePool{
		storagePoolKey: storagePoolKey,
		hosts:          hosts,
		port:           port,
		minConns:       storageMinConn,
		maxConns:       storageMaxConn,
	}
	storagePoolChan <- spd

	select {
	case result = <-fetchStoragePoolChan:
		var storagePool *ConnectionPool
		if err, ok = result.(error); ok {
			return nil, err
		} else if storagePool, ok = result.(*ConnectionPool); ok {
			return storagePool, nil
		} else {
			return nil, errors.New("none")
		}
	}
}
