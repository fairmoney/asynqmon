package asynqmon

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/hibiken/asynq"
)

// ****************************************************************************
// This file defines:
//   - http.Handler(s) for scheduler entry related endpoints
// ****************************************************************************

func newListSchedulerEntriesHandlerFunc(inspector *asynq.Inspector, pf PayloadFormatter, allowRunNow func(string) bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entries, err := inspector.SchedulerEntries()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		payload := make(map[string]interface{})
		if len(entries) == 0 {
			// avoid nil for the entries field in json output.
			payload["entries"] = make([]*schedulerEntry, 0)
		} else {
			out := toSchedulerEntries(entries, pf)
			if allowRunNow != nil {
				for _, e := range out {
					e.RunAllowed = allowRunNow(e.TaskType)
				}
			}
			payload["entries"] = out
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

type listSchedulerEnqueueEventsResponse struct {
	Events []*schedulerEnqueueEvent `json:"events"`
}

func newListSchedulerEnqueueEventsHandlerFunc(inspector *asynq.Inspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entryID := mux.Vars(r)["entry_id"]
		pageSize, pageNum := getPageOptions(r)
		events, err := inspector.ListSchedulerEnqueueEvents(
			entryID, asynq.PageSize(pageSize), asynq.Page(pageNum))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := listSchedulerEnqueueEventsResponse{
			Events: toSchedulerEnqueueEvents(events),
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

type runSchedulerEntryResponse struct {
	TaskID   string `json:"task_id"`
	Queue    string `json:"queue"`
	TaskType string `json:"task_type"`
}

// runNowCSRFHeader must be set to runNowCSRFValue on run-now requests. A cross-site
// HTML form cannot set a custom header, and a cross-site script that sets one
// needs a CORS preflight, which this server does not allow.
const (
	runNowCSRFHeader = "X-Requested-With"
	runNowCSRFValue  = "XMLHttpRequest"
)

// newRunSchedulerEntryHandlerFunc returns a handler that enqueues the task of a
// scheduler entry once, with the same type, payload and options as the scheduler uses.
func newRunSchedulerEntryHandlerFunc(inspector *asynq.Inspector, client *asynq.Client, allowRunNow func(string) bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(runNowCSRFHeader) != runNowCSRFValue {
			http.Error(w, fmt.Sprintf("%s: %s header is required", http.StatusText(http.StatusForbidden), runNowCSRFHeader), http.StatusForbidden)
			return
		}
		entryID := mux.Vars(r)["entry_id"]
		entries, err := inspector.SchedulerEntries()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var entry *asynq.SchedulerEntry
		for _, e := range entries {
			if e.ID == entryID {
				entry = e
				break
			}
		}
		if entry == nil {
			http.Error(w, fmt.Sprintf("scheduler entry %q not found", entryID), http.StatusNotFound)
			return
		}
		if !allowRunNow(entry.Task.Type()) {
			http.Error(w, fmt.Sprintf("task type %q is not allowed to run on demand", entry.Task.Type()), http.StatusForbidden)
			return
		}
		info, err := client.EnqueueContext(r.Context(), entry.Task, entry.Opts...)
		switch {
		case errors.Is(err, asynq.ErrDuplicateTask), errors.Is(err, asynq.ErrTaskIDConflict):
			log.Printf("asynqmon: run now rejected as duplicate: entry_id=%s task_type=%s caller=%s", entryID, entry.Task.Type(), runNowCaller(r))
			http.Error(w, err.Error(), http.StatusConflict)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("asynqmon: run now: entry_id=%s task_type=%s task_id=%s queue=%s caller=%s", entryID, info.Type, info.ID, info.Queue, runNowCaller(r))
		resp := runSchedulerEntryResponse{TaskID: info.ID, Queue: info.Queue, TaskType: info.Type}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// runNowCaller describes who sent a run-now request, for the audit log. Asynqmon
// has no auth, so this is the network address, plus X-Forwarded-For when a proxy sets it.
func runNowCaller(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return fmt.Sprintf("%s (x-forwarded-for: %s)", r.RemoteAddr, fwd)
	}
	return r.RemoteAddr
}
