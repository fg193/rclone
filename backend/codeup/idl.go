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

const hexAlphabet = "0123456789ABCDEF"

// Return true if the specified character should be escaped when
// appearing in a URL string, according to RFC 3986.
//
// Please be informed that for now shouldEscape does not check all
// reserved characters correctly. See golang.org/issue/5684.
func shouldEscape(c byte) bool {
	// §2.3 Unreserved characters (alphanum)
	if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' {
		return false
	}
	switch c {
	// §2.3 Unreserved characters (mark)
	case '-', '_', '.', '~':
		return false

	// §2.2 Reserved characters (reserved)
	case ' ', '#', '%', '&', '+', ';', '?':
		return true
	case '$', ',', '/', ':', '=', '@':
		return false
	}

	// Everything else should be escaped.
	return true
}

func escape(s string) (ret []byte) {
	hexCount := 0
	for i := 0; i < len(s); i++ {
		if shouldEscape(s[i]) {
			hexCount++
		}
	}
	if hexCount == 0 {
		return []byte(s)
	}

	var buf [64]byte
	required := len(s) + 2*hexCount
	if required <= len(buf) {
		ret = buf[:required]
	} else {
		ret = make([]byte, required)
	}

	for i, j := 0, 0; i < len(s); i++ {
		switch c := s[i]; {
		case shouldEscape(c):
			ret[j+0] = '%'
			ret[j+1] = hexAlphabet[c>>4]
			ret[j+2] = hexAlphabet[c&15]
			j += 3
		default:
			ret[j] = s[i]
			j++
		}
	}
	return ret
}
