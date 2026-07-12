package webdav

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/service"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type Handler struct {
	Store    *store.Store
	Streamer *service.Streamer
	Prefix   string
}
type multiStatus struct {
	XMLName   xml.Name      `xml:"D:multistatus"`
	D         string        `xml:"xmlns:D,attr"`
	Responses []davResponse `xml:"D:response"`
}
type davResponse struct {
	Href     string   `xml:"D:href"`
	PropStat propStat `xml:"D:propstat"`
}
type propStat struct {
	Prop   davProp `xml:"D:prop"`
	Status string  `xml:"D:status"`
}
type davProp struct {
	DisplayName  string       `xml:"D:displayname"`
	ResourceType resourceType `xml:"D:resourcetype"`
	Length       int64        `xml:"D:getcontentlength,omitempty"`
	Modified     string       `xml:"D:getlastmodified"`
	ContentType  string       `xml:"D:getcontenttype,omitempty"`
	ETag         string       `xml:"D:getetag,omitempty"`
}
type resourceType struct {
	Collection *struct{} `xml:"D:collection,omitempty"`
}
type entry struct {
	Path string
	Dir  bool
	File *model.File
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("DAV", "1")
	w.Header().Set("MS-Author-Via", "DAV")
	p, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, h.Prefix))
	if err != nil {
		http.Error(w, "bad path", 400)
		return
	}
	p = strings.Trim(path.Clean("/"+p), "/")
	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Allow", "OPTIONS, GET, HEAD, PROPFIND")
		w.WriteHeader(200)
	case "PROPFIND":
		h.propfind(w, r, p)
	case http.MethodGet, http.MethodHead:
		f, ok := h.Store.FindPath(p)
		if !ok {
			http.NotFound(w, r)
			return
		}
		h.Streamer.Serve(w, r, f)
	default:
		w.Header().Set("Allow", "OPTIONS, GET, HEAD, PROPFIND")
		http.Error(w, "read-only", http.StatusMethodNotAllowed)
	}
}
func (h *Handler) propfind(w http.ResponseWriter, r *http.Request, p string) {
	entries := h.entries()
	current, ok := entries[p]
	if !ok {
		http.NotFound(w, r)
		return
	}
	selected := []entry{current}
	if r.Header.Get("Depth") != "0" && current.Dir {
		prefix := p
		if prefix != "" {
			prefix += "/"
		}
		for k, v := range entries {
			if k == p || !strings.HasPrefix(k, prefix) {
				continue
			}
			if !strings.Contains(strings.TrimPrefix(k, prefix), "/") {
				selected = append(selected, v)
			}
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Path < selected[j].Path })
	ms := multiStatus{D: "DAV:"}
	for _, e := range selected {
		href := h.Prefix
		if !strings.HasSuffix(href, "/") {
			href += "/"
		}
		if e.Path != "" {
			parts := strings.Split(e.Path, "/")
			for i := range parts {
				parts[i] = url.PathEscape(parts[i])
			}
			href += strings.Join(parts, "/")
		}
		if e.Dir && !strings.HasSuffix(href, "/") {
			href += "/"
		}
		name := path.Base(e.Path)
		if e.Path == "" {
			name = "WatchTower"
		}
		prop := davProp{DisplayName: name, Modified: time.Unix(0, 0).UTC().Format(http.TimeFormat)}
		if e.Dir {
			prop.ResourceType.Collection = &struct{}{}
		} else {
			prop.Length = e.File.Size
			prop.ContentType = "application/octet-stream"
			prop.ETag = fmt.Sprintf(`"%s-%d"`, e.File.ID, e.File.Size)
		}
		ms.Responses = append(ms.Responses, davResponse{Href: href, PropStat: propStat{Prop: prop, Status: "HTTP/1.1 200 OK"}})
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(207)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(ms)
}
func (h *Handler) entries() map[string]entry {
	out := map[string]entry{"": {Path: "", Dir: true}}
	for _, f := range h.Store.Files() {
		p := strings.Trim(f.Path, "/")
		out[p] = entry{Path: p, File: f}
		parts := strings.Split(p, "/")
		for i := 1; i < len(parts); i++ {
			d := strings.Join(parts[:i], "/")
			out[d] = entry{Path: d, Dir: true}
		}
	}
	return out
}
