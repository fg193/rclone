package coding

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/rclone/rclone/lib/rest"
)

// https://help.coding.net/openapi
type (
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

	ReleaseStrategyOverwrite ReleaseStrategy = iota + 1
	ReleaseStrategyNoClobber
	ReleaseStrategySnapshot
)

type Request interface {
	ActionName() string
}

type Page struct {
	PageNumber int `json:",omitempty"`
	PageSize   int `json:",omitempty"`
	TotalCount int `json:",omitempty"`
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
	Id        uintptr
	RequestId string
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
	Data      ArtifactRepositoryPageBean
	RequestId string
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
	Data      ArtifactPackagePageBean
	RequestId string
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
	Data      ArtifactVersionPageBean
	RequestId string
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
	Url       string
	RequestId string
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
	InstanceSet []ArtifactProperty
	RequestId   string
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
	RequestId string
}

func call(
	ctx context.Context,
	rpc *rest.Client,
	req Request,
	resp interface{},
) (*http.Response, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	reqMap := map[string]interface{}{
		"Action": req.ActionName(),
	}
	if err = json.Unmarshal(reqBytes, &reqMap); err != nil {
		return nil, err
	}

	return rpc.CallJSON(ctx, &rest.Opts{Method: http.MethodPost}, req, resp)
}
