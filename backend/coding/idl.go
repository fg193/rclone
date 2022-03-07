package coding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/rclone/rclone/lib/rest"
)

// https://help.coding.net/openapi
type (
	ProjectStatus   uint8
	ProjectType     uint8
	AccessLevel     uint8
	RepositoryType  uint8
	ReleaseStrategy uint8
	ReleaseStatus   uint8
	Timestamp       int64
)

const (
	AccessLevelProject AccessLevel = iota + 1
	AccessLevelTeam
	AccessLevelPublic
)

const (
	RepositoryTypeGeneric RepositoryType = iota + 1
	RepositoryTypeDocker
	RepositoryTypeMaven
	RepositoryTypeNPM
	RepositoryTypePyPI
	RepositoryTypeHelm
	RepositoryTypeComposer
	RepositoryTypeNuGet
	RepositoryTypeConan
	RepositoryTypeCocoaPods
	RepositoryTypeRPM
)

const (
	ReleaseStrategyOverwrite ReleaseStrategy = iota + 1
	ReleaseStrategyNoClobber
	ReleaseStrategySnapshot

	LatestVersion = "latest"
)

type ActionRequest interface {
	ActionName() string
}

type MethodRequest interface {
	MethodName() string
}

type Page struct {
	PageNumber int `json:",omitempty"`
	PageSize   int `json:",omitempty"`
	TotalCount int `json:",omitempty"`
}

type BaseResponse struct {
	Error struct {
		Message string
		Code    string
	} `json:",omitempty"`
	RequestId string `json:",omitempty"`
}

func (DescribeCodingProjectsRequest) ActionName() string {
	return "DescribeCodingProjects"
}

type DescribeCodingProjectsRequest struct {
	ProjectName string `json:",omitempty"`
	Page
}

type DescribeCodingProjectsResponse struct {
	Data ProjectData
	BaseResponse
}

type ProjectData struct {
	ProjectList []Project
	Page
}

type Project struct {
	Id          uintptr
	CreatedAt   Timestamp
	UpdatedAt   Timestamp
	Status      ProjectStatus
	Type        ProjectType
	MaxMember   int
	Name        string
	DisplayName string
	Description string
	Icon        string
	TeamOwnerId uintptr
	UserOwnerId uintptr
	StartDate   Timestamp
	EndDate     Timestamp
	TeamId      uintptr
	IsDemo      bool
	Archived    bool
}

func (CreateArtifactRepositoryRequest) ActionName() string {
	return "CreateArtifactRepository"
}

type CreateArtifactRepositoryRequest struct {
	ProjectId      uintptr
	RepositoryName string
	Type           RepositoryType
	Description    string      `json:",omitempty"`
	AccessLevel    AccessLevel `json:",omitempty"`
	AllowProxy     bool        `json:",omitempty"`
}

type CreateArtifactRepositoryResponse struct {
	Id           uintptr
	BaseResponse `json:",omitempty"`
}

func (DescribeArtifactRepositoryListRequest) ActionName() string {
	return "DescribeArtifactRepositoryList"
}

type DescribeArtifactRepositoryListRequest struct {
	ProjectId uintptr
	Type      RepositoryType `json:",omitempty"`
	Page
}

type DescribeArtifactRepositoryListResponse struct {
	Data         ArtifactRepositoryPageBean
	BaseResponse `json:",omitempty"`
}

type ArtifactRepositoryPageBean struct {
	InstanceSet []ArtifactRepositoryBean
	Page
}

// ArtifactRepositoryBean describes a Tencent COS bucket
type ArtifactRepositoryBean struct {
	Id              uintptr
	Name            string
	TeamId          uintptr
	ProjectId       uintptr
	Type            RepositoryType
	Description     *string
	AccessLevel     AccessLevel
	ReleaseStrategy ReleaseStrategy
	CreatedAt       Timestamp
}

func (t Timestamp) Into() time.Time {
	return time.Unix(int64(t), 0)
}

func (DescribeArtifactPackageListRequest) ActionName() string {
	return "DescribeArtifactPackageList"
}

type DescribeArtifactPackageListRequest struct {
	ProjectId     uintptr
	Repository    string
	PackagePrefix string `json:",omitempty"`
	Page
}

type DescribeArtifactPackageListResponse struct {
	Data         ArtifactPackagePageBean
	BaseResponse `json:",omitempty"`
}

type ArtifactPackagePageBean struct {
	InstanceSet []ArtifactPackageBean
	Page
}

// ArtifactPackageBean describes a file
type ArtifactPackageBean struct {
	Id           uintptr
	Name         string
	RepoId       uintptr
	Description  string
	CreatedAt    Timestamp
	VersionCount int

	LatestVersionId            uintptr
	LatestVersionName          string
	LatestVersionReleaseStatus ReleaseStatus
	ReleaseStrategy            ReleaseStrategy
}

func (DescribeArtifactVersionListRequest) ActionName() string {
	return "DescribeArtifactVersionList"
}

type DescribeArtifactVersionListRequest struct {
	ProjectId  uintptr
	Repository string
	Package    string
	Page
}

type DescribeArtifactVersionListResponse struct {
	Data         ArtifactVersionPageBean
	BaseResponse `json:",omitempty"`
}

type ArtifactVersionPageBean struct {
	InstanceSet []ArtifactVersionBean
	Page
}

type ArtifactVersionBean struct {
	Id            uintptr
	Version       string
	Hash          string
	Size          float64
	Description   string
	PkgId         uintptr
	DownloadCount int
	CreatedAt     Timestamp
	ReleaseStatus ReleaseStatus
}

func (DescribeArtifactFileDownloadUrlRequest) ActionName() string {
	return "DescribeArtifactFileDownloadUrl"
}

type DescribeArtifactFileDownloadUrlRequest struct {
	ProjectId      uintptr
	Repository     string
	Package        string
	PackageVersion string
	FileName       string
	Timeout        Timestamp `json:",omitempty"`
}

type DescribeArtifactFileDownloadUrlResponse struct {
	Url          string
	BaseResponse `json:",omitempty"`
}

func (DescribeArtifactPropertiesRequest) ActionName() string {
	return "DescribeArtifactProperties"
}

type DescribeArtifactPropertiesRequest struct {
	ProjectId      uintptr
	Repository     string
	Package        string
	PackageVersion string
}

type DescribeArtifactPropertiesResponse struct {
	InstanceSet  []ArtifactProperty
	BaseResponse `json:",omitempty"`
}

type ArtifactProperty struct {
	Id        uintptr
	Version   string
	CreatedAt Timestamp
	Immutable bool
	ArtifactPropertyBean
}

type ArtifactPropertyBean struct {
	Name  string
	Value string
}

func (CreateArtifactPropertiesRequest) ActionName() string {
	return "CreateArtifactProperties"
}

type CreateArtifactPropertiesRequest ModifyArtifactPropertiesRequest

func (ModifyArtifactPropertiesRequest) ActionName() string {
	return "ModifyArtifactProperties"
}

type ModifyArtifactPropertiesRequest struct {
	ProjectId      uintptr
	Repository     string
	Package        string
	PackageVersion string
	PropertySet    []ArtifactPropertyBean
}

func (DeleteArtifactPropertiesRequest) ActionName() string {
	return "DeleteArtifactProperties"
}

type DeleteArtifactPropertiesRequest struct {
	ProjectId       uintptr
	Repository      string
	Package         string
	PackageVersion  string
	PropertyNameSet []string
}

type CreateArtifactPropertiesResponse ModifyArtifactPropertiesResponse
type DeleteArtifactPropertiesResponse ModifyArtifactPropertiesResponse

type ModifyArtifactPropertiesResponse struct {
	BaseResponse `json:",omitempty"`
}

func (GetArtifactVersionExistChunksRequest) MethodName() string {
	return http.MethodPost
}

func (GetArtifactVersionExistChunksRequest) ActionName() string {
	return "part-init"
}

type GetArtifactVersionExistChunksRequest struct {
	Version  string `json:"version"`
	FileTag  string `json:"fileTag"`
	FileSize int64  `json:"fileSize,string"`
}

type GetArtifactVersionExistChunksResponse struct {
	Data struct {
		UploadId string `json:"uploadId"`
	} `json:"data"`
}

func (UploadArtifactVersionChunkRequest) MethodName() string {
	return http.MethodPost
}

func (UploadArtifactVersionChunkRequest) ActionName() string {
	return "part-upload"
}

type UploadArtifactVersionChunkRequest struct {
	Version    string `json:"version"`
	UploadId   string `json:"uploadId"`
	PartNumber int    `json:"partNumber,string`
	ChunkSize  int64  `json:"size,string"`
}

func (MergeArtifactVersionChunksRequest) MethodName() string {
	return http.MethodPost
}

func (MergeArtifactVersionChunksRequest) ActionName() string {
	return "part-complete"
}

type MergeArtifactVersionChunksRequest struct {
	Version  string `json:"version"`
	UploadId string `json:"uploadId"`
	FileTag  string `json:"fileTag"`
	FileSize int64  `json:"size,string"`
}

func (UploadArtifactVersionRequest) MethodName() string {
	return http.MethodPut
}

type UploadArtifactVersionRequest DeleteArtifactVersionRequest

func (DeleteArtifactVersionRequest) MethodName() string {
	return http.MethodDelete
}

type DeleteArtifactVersionRequest struct {
	Version string `json:"version"`
}

// call Coding RPC style open API with plain text token
func (f *Fs) call(
	ctx context.Context,
	req ActionRequest,
	resp interface{},
) (*http.Response, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	reqMap := map[string]interface{}{
		"Action": req.ActionName(),
	}
	if err = jsonUnmarshalUseNumber(reqBytes, &reqMap); err != nil {
		return nil, err
	}

	respWrapper := struct{ Response interface{} }{Response: resp}
	opts := rest.Opts{
		Method: http.MethodPost,
		ExtraHeaders: map[string]string{
			"Authorization": "token" + f.opt.Token,
		},
	}
	ret, err := f.srvRest.CallJSON(ctx, &opts, &reqMap, &respWrapper)
	if err != nil {
		return ret, err
	}
	// fmt.Printf("\n\nreq: %+v\n\nresp: %#v\n\n", reqMap, resp)

	errorCode := reflect.
		ValueOf(respWrapper.Response).
		Elem().
		FieldByName("BaseResponse").
		FieldByName("Error").
		FieldByName("Code").
		String()

	if len(errorCode) > 0 {
		return ret, fmt.Errorf("%s: %s", req.ActionName(), errorCode)
	}
	return ret, err
}

// call Coding RESTful open API with basic authentication
func (f *Fs) callRest(
	ctx context.Context,
	path string,
	req MethodRequest,
	resp interface{},
) (*http.Response, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var reqMap map[string]string
	if err = jsonUnmarshalUseNumber(reqBytes, &reqMap); err != nil {
		return nil, err
	}

	var reqValues url.Values
	for k, v := range reqMap {
		reqValues.Set(k, v)
	}
	if reqAction, ok := req.(ActionRequest); ok {
		reqValues.Set("action", reqAction.ActionName())
	}
	opts := rest.Opts{
		Method:     req.MethodName(),
		Password:   f.opt.Token,
		Parameters: reqValues,
	}
	return f.srvRest.CallJSON(ctx, &opts, nil, &resp)
}

func jsonUnmarshalUseNumber(blob []byte, ret interface{}) error {
	decoder := json.NewDecoder(bytes.NewBuffer(blob))
	decoder.UseNumber()
	return decoder.Decode(&ret)
}
