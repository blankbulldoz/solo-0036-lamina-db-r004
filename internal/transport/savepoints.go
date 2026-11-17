package transport

import "net/http"

func (s *Server) savepoint(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.CreateSavepoint(r.PathValue("tx"), in.Name)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 201, out)
}

func (s *Server) rollbackSavepoint(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.RollbackSavepoint(r.PathValue("tx"), in.Name)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) releaseSavepoint(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.ReleaseSavepoint(r.PathValue("tx"), in.Name)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) listSavepoints(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.ListSavepoints(r.PathValue("tx"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
