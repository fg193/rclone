package codeup

import (
	"context"
	"encoding/json"
	"errors"
	"html/template"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rclone/rclone/cmd/serve/http/data"
	"github.com/rclone/rclone/fs"
	httplib "github.com/rclone/rclone/lib/http"
	"github.com/rclone/rclone/lib/http/auth"
	"github.com/rclone/rclone/lib/http/serve"
)

func (f *Fs) serve(ctx context.Context, opts map[string]string) (err error) {
	if len(opts) > 0 {
		var marshaled []byte
		if marshaled, err = json.Marshal(&opts); err != nil {
			return
		}
		customOpts := httplib.GetOptions()
		if err = json.Unmarshal(marshaled, &customOpts); err != nil {
			return
		}
		httplib.SetOptions(customOpts)
	}

	htmlTemplate, err := data.GetTemplate(opts["Template"])
	if err != nil {
		return
	}
	s := &server{
		fs:           f,
		htmlTemplate: htmlTemplate,
	}

	router, err := httplib.Router()
	if err != nil {
		return
	}
	s.Bind(router)

	httplib.Wait()
	return
}

type server struct {
	fs           *Fs
	htmlTemplate *template.Template
}

func (s *server) Bind(router chi.Router) {
	if m := auth.Auth(auth.Opt); m != nil {
		router.Use(m)
	}
	router.Use(
		middleware.RealIP,
		middleware.Logger,
		middleware.CleanPath,
		middleware.GetHead,
		middleware.SetHeader("Server", "reflector/"+fs.Version),
	)
	router.Get("/*", s.handler)
}

// handler reads incoming requests and dispatches them
func (s *server) handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	reqPathIsDir := strings.HasSuffix(r.URL.Path, "/")
	remote := strings.Trim(r.URL.Path, "/")

	o, err := s.fs.getObject(ctx, remote)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, fs.ErrorObjectNotFound) {
			statusCode = http.StatusNotFound
		}

		http.Error(w, err.Error(), statusCode)
		return
	}

	// the slash suffix is required for correct relative links
	if !reqPathIsDir && o.Mode.IsDir() {
		http.Redirect(w, r, remote+"/", http.StatusMovedPermanently)
		return
	}

	switch {
	case o.Mode.IsDir():
		err = s.serveDir(ctx, w, r, o)
	case o.Mode.IsRegular():
		err = s.serveFile(ctx, w, r, o)
	case o.Mode&os.ModeSymlink > 0:
		err = s.serveSymLink(ctx, w, r, o)
	default:
		err = s.serveInline(ctx, w, r, o)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) serveDir(ctx context.Context, w http.ResponseWriter, r *http.Request, dir *Object) (err error) {
	dirRemote := dir.remote(dir.FileName)
	objects, err := s.fs.list(ctx, dirRemote)
	if err != nil {
		return
	}

	directory := serve.NewDirectory(dirRemote, s.htmlTemplate)
	for _, o := range objects {
		directory.AddHTMLEntry(
			o.remote(o.FileName),
			o.Mode.IsDir(),
			o.Size(),
			o.ModTime(ctx).UTC(),
		)
	}

	sortParm := r.URL.Query().Get("sort")
	orderParm := r.URL.Query().Get("order")
	directory.ProcessQueryParams(sortParm, orderParm)

	// Set the Last-Modified header to the timestamp
	w.Header().Set("Last-Modified", dir.ModTime(ctx).UTC().Format(http.TimeFormat))

	directory.Serve(w, r)
	return nil
}

func (s *server) serveFile(ctx context.Context, w http.ResponseWriter, r *http.Request, o *Object) (err error) {
	w.Header().Set("Last-Modified", o.ModTime(ctx).UTC().Format(http.TimeFormat))

	// If HEAD no need to read the object since we have set the headers
	if r.Method == "HEAD" {
		return
	}

	url, err := (&RegularFile{*o}).getLink(ctx)
	if err != nil {
		return
	}

	http.Redirect(w, r, url, http.StatusFound)
	return
}

func (s *server) serveSymLink(ctx context.Context, w http.ResponseWriter, r *http.Request, o *Object) (err error) {
	target := string(o.Contents)

	// client redirect to external scheme://host
	u, err := url.Parse(target)
	if err != nil || len(u.Scheme) > 0 || len(u.Host) > 0 {
		w.Header().Set("Location", target)
		w.WriteHeader(http.StatusFound)
		return nil
	}

	// server internal redirect

	// make relative path absolute by combining with request path
	if len(u.Path) <= 0 || u.Path[0] != '/' {
		oldDir, _ := path.Split(r.URL.Path)
		u.Path = oldDir + u.Path
	}

	// clean up but preserve trailing slash
	trailing := strings.HasSuffix(u.Path, "/")
	u.Path = path.Clean(u.Path)
	if trailing && !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}

	r.URL.Path = u.Path
	r.URL.RawQuery = u.RawQuery
	s.handler(w, r)
	return nil
}

func (s *server) serveInline(ctx context.Context, w http.ResponseWriter, r *http.Request, o *Object) (err error) {
	w.Header().Set("Content-Length", strconv.Itoa(len(o.Contents)))
	w.Header().Set("Last-Modified", o.ModTime(ctx).UTC().Format(http.TimeFormat))

	if r.Method == "HEAD" {
		return
	}

	if err = o.lazyLoad(ctx); err != nil {
		return
	}
	_, err = w.Write(o.Contents)
	return
}
