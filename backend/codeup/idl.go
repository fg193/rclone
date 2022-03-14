package codeup

import (
	"net/url"
	"strconv"
)

type BaseResponse struct {
	Successful bool   `json:"successful"`
	ErrorCode  string `json:"errorCode,omitempty"`
	ErrorMsg   string `json:"errorMsg,omitempty"`
}

type CreateVersionResponse struct {
	BaseResponse
	Object struct {
		Hashes
		FileSize int64  `json:"fileSize"`
		URL      string `json:"url"`
	} `json:"object"`
}

type DeleteVersionResponse struct {
	BaseResponse
	Object bool `json:"object"`
}

func NewVersionParams(version int64) (ret url.Values) {
	ret = make(url.Values)
	ret.Set("version", strconv.FormatInt(version, 10))
	return
}
