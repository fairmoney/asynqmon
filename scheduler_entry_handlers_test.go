package asynqmon

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const runNowPathTemplate = "/api/scheduler_entries/{entry_id}/run"

// testRedisOpt returns the redis connection for the tests that need a redis-server.
// Set ASYNQMON_TEST_REDIS_ADDR to override the default address (localhost:6379).
// The test is skipped if redis is not reachable.
func testRedisOpt(t *testing.T) asynq.RedisClientOpt {
	t.Helper()
	addr := os.Getenv("ASYNQMON_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	opt := asynq.RedisClientOpt{Addr: addr, DB: 14}
	rc := opt.MakeRedisClient().(redis.UniversalClient)
	defer rc.Close()
	if err := rc.FlushDB(t.Context()).Err(); err != nil {
		t.Skipf("redis is not available at %s: %v", addr, err)
	}
	return opt
}

// startScheduler registers the given tasks with a scheduler and waits until the entries are visible in redis.
func startScheduler(t *testing.T, opt asynq.RedisClientOpt, register func(s *asynq.Scheduler)) {
	t.Helper()
	s := asynq.NewScheduler(opt, &asynq.SchedulerOpts{HeartbeatInterval: 100 * time.Millisecond, LogLevel: asynq.ErrorLevel})
	register(s)
	if err := s.Start(); err != nil {
		t.Fatalf("scheduler.Start: %v", err)
	}
	t.Cleanup(s.Shutdown)
	insp := asynq.NewInspector(opt)
	defer insp.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if entries, err := insp.SchedulerEntries(); err == nil && len(entries) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("scheduler entries did not appear in redis")
}

func hasRoute(t *testing.T, h *HTTPHandler, tmpl, method string) bool {
	t.Helper()
	found := false
	err := h.router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		p, err := route.GetPathTemplate()
		if err != nil || !strings.HasSuffix(p, tmpl) {
			return nil
		}
		methods, _ := route.GetMethods()
		for _, m := range methods {
			if m == method {
				found = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("router.Walk: %v", err)
	}
	return found
}

func allowOnly(types ...string) func(string) bool {
	return func(taskType string) bool {
		for _, t := range types {
			if t == taskType {
				return true
			}
		}
		return false
	}
}

func TestRunSchedulerEntryRouteAbsentWhenAllowRunNowIsNil(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		h := New(Options{RedisConnOpt: asynq.RedisClientOpt{Addr: "localhost:6379"}, ReadOnly: readOnly})
		if hasRoute(t, h, runNowPathTemplate, "POST") {
			t.Errorf("ReadOnly=%v: run-now route is registered, want absent when AllowRunNow is nil", readOnly)
		}
		h.Close()
	}
}

func TestRunSchedulerEntryRoutePresentWhenAllowRunNowIsSet(t *testing.T) {
	h := New(Options{RedisConnOpt: asynq.RedisClientOpt{Addr: "localhost:6379"}, ReadOnly: true, AllowRunNow: allowOnly()})
	defer h.Close()
	if !hasRoute(t, h, runNowPathTemplate, "POST") {
		t.Error("run-now route is not registered, want present when AllowRunNow is set")
	}
}

func TestRunSchedulerEntry(t *testing.T) {
	opt := testRedisOpt(t)
	startScheduler(t, opt, func(s *asynq.Scheduler) {
		if _, err := s.Register("@every 1h", asynq.NewTask("allowed:task", []byte(`{"a":1}`)),
			asynq.Queue("critical"), asynq.MaxRetry(3), asynq.Timeout(30*time.Second), asynq.Unique(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Register("@every 1h", asynq.NewTask("blocked:task", nil)); err != nil {
			t.Fatal(err)
		}
	})

	h := New(Options{RootPath: "/monitoring", RedisConnOpt: opt, ReadOnly: true, AllowRunNow: allowOnly("allowed:task")})
	defer h.Close()
	insp := asynq.NewInspector(opt)
	defer insp.Close()

	do := func(method, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, "/monitoring"+path, nil))
		return rec
	}

	// List response carries run_allowed per entry.
	rec := do("GET", "/api/scheduler_entries")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: status = %d, want 200; body = %s", rec.Code, rec.Body)
	}
	var list struct {
		Entries []schedulerEntry `json:"entries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, e := range list.Entries {
		ids[e.TaskType] = e.ID
		if want := e.TaskType == "allowed:task"; e.RunAllowed != want {
			t.Errorf("list: %s run_allowed = %v, want %v", e.TaskType, e.RunAllowed, want)
		}
	}

	// ReadOnly still blocks the other mutating routes.
	if rec := do("POST", "/api/queues/critical:pause"); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("pause queue under ReadOnly: status = %d, want 405", rec.Code)
	}

	// Unknown entry.
	if rec := do("POST", "/api/scheduler_entries/does-not-exist/run"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown entry: status = %d, want 404; body = %s", rec.Code, rec.Body)
	}

	// Disallowed task type.
	if rec := do("POST", "/api/scheduler_entries/"+ids["blocked:task"]+"/run"); rec.Code != http.StatusForbidden {
		t.Errorf("disallowed type: status = %d, want 403; body = %s", rec.Code, rec.Body)
	}

	// Success: the task is enqueued with the entry's payload and options.
	rec = do("POST", "/api/scheduler_entries/"+ids["allowed:task"]+"/run")
	if rec.Code != http.StatusOK {
		t.Fatalf("run: status = %d, want 200; body = %s", rec.Code, rec.Body)
	}
	var resp runSchedulerEntryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Queue != "critical" || resp.TaskType != "allowed:task" || resp.TaskID == "" {
		t.Errorf("run: response = %+v", resp)
	}
	info, err := insp.GetTaskInfo("critical", resp.TaskID)
	if err != nil {
		t.Fatalf("GetTaskInfo: %v", err)
	}
	if info.Type != "allowed:task" || string(info.Payload) != `{"a":1}` || info.MaxRetry != 3 ||
		info.Timeout != 30*time.Second || info.State != asynq.TaskStatePending {
		t.Errorf("enqueued task = %+v, want entry's type, payload and options", info)
	}

	// Duplicate: the Unique option of the entry rejects a second run.
	if rec := do("POST", "/api/scheduler_entries/"+ids["allowed:task"]+"/run"); rec.Code != http.StatusConflict {
		t.Errorf("duplicate run: status = %d, want 409; body = %s", rec.Code, rec.Body)
	}
	if tasks, _ := insp.ListPendingTasks("critical"); len(tasks) != 1 {
		t.Errorf("pending tasks in critical = %d, want 1", len(tasks))
	}
}
