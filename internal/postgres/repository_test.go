package postgres

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// Интеграционные тесты репозитория на настоящем PostgreSQL. Фейки в тестах
// сервиса проверяют логику, а здесь проверяется сам SQL: имена колонок,
// ON CONFLICT, атомарность транзакции Outbox, ограничения схемы.
//
// Запуск:
//
//	docker run -d --name pg -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_DATABASE_DSN='postgres://postgres@localhost:5432/postgres?sslmode=disable' go test ./internal/postgres/
//
// Без TEST_DATABASE_DSN тесты пропускаются. Каждый тест работает в своей
// схеме, поэтому тесты не мешают друг другу.
const dsnEnv = "TEST_DATABASE_DSN"

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s не задан — интеграционный тест пропущен", dsnEnv)
	}

	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })

	schema := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatalf("создание схемы: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE") })

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("postgres", dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	applyMigrations(t, db, "up")
	return db
}

// applyMigrations выполняет migrations/*.<direction>.sql: up — по возрастанию
// номера, down — по убыванию.
func applyMigrations(t *testing.T, db *sql.DB, direction string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("migrations", "*."+direction+".sql"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("не найдены миграции *.%s.sql", direction)
	}
	sort.Strings(files)
	if direction == "down" {
		for i, j := 0, len(files)-1; i < j; i, j = i+1, j-1 {
			files[i], files[j] = files[j], files[i]
		}
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("миграция %s: %v", f, err)
		}
	}
}

func newRepo(t *testing.T, db *sql.DB) *Repository {
	t.Helper()
	r, err := NewRepository(db, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func insertApp(t *testing.T, db *sql.DB, status limitservice.ApplicationStatus) limitservice.Application {
	t.Helper()
	app := limitservice.Application{
		ID:                   uuid.NewString(),
		ClientID:             uuid.NewString(),
		LenderID:             uuid.NewString(),
		Status:               status,
		RequestedAmountMinor: 150_000_00,
		Currency:             "RUB",
		Version:              1,
	}
	if status == limitservice.LimitBlocked || status == limitservice.ContractSigned {
		app.LenderBlockID = "blk-existing"
	}
	_, err := db.Exec(`INSERT INTO applications
		(id, client_id, lender_id, status, requested_amount_minor, currency, lender_block_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		app.ID, app.ClientID, app.LenderID, app.Status, app.RequestedAmountMinor, app.Currency, app.LenderBlockID, app.Version)
	if err != nil {
		t.Fatalf("вставка заявки: %v", err)
	}
	return app
}

type opRow struct {
	OperationID string
	State       string
	BlockID     string
}

func readOp(t *testing.T, db *sql.DB, appID string) (opRow, bool) {
	t.Helper()
	var r opRow
	err := db.QueryRow(`SELECT operation_id, state, block_id FROM limit_block_operation WHERE application_id = $1`, appID).
		Scan(&r.OperationID, &r.State, &r.BlockID)
	if errors.Is(err, sql.ErrNoRows) {
		return opRow{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return r, true
}

func readApp(t *testing.T, r *Repository, id string) limitservice.Application {
	t.Helper()
	app, err := r.FindByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func countOutbox(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func message(appID string) limitservice.KafkaMessage {
	id := uuid.NewString()
	return limitservice.KafkaMessage{
		EventID: id,
		Topic:   "credit.application.events",
		Key:     appID,
		Payload: []byte(`{"event_id":"` + id + `","application_id":"` + appID + `"}`),
	}
}

// --- миграции ---

func TestMigrations_UpDownUp(t *testing.T) {
	db := testDB(t) // up уже применён
	applyMigrations(t, db, "down")
	applyMigrations(t, db, "up")
}

func TestMigrations_Constraints(t *testing.T) {
	db := testDB(t)
	bad := []struct {
		name  string
		query string
		args  []any
	}{
		{"сумма 0", `INSERT INTO applications (id, client_id, lender_id, status, requested_amount_minor, currency, version)
			VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'INITIAL', 0, 'RUB', 1)`, nil},
		{"неизвестный статус", `INSERT INTO applications (id, client_id, lender_id, status, requested_amount_minor, currency, version)
			VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'APPROVED', 100, 'RUB', 1)`, nil},
		{"валюта USD", `INSERT INTO applications (id, client_id, lender_id, status, requested_amount_minor, currency, version)
			VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'INITIAL', 100, 'USD', 1)`, nil},
		{"LIMIT_BLOCKED без номера блокировки", `INSERT INTO applications (id, client_id, lender_id, status, requested_amount_minor, currency, version)
			VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'LIMIT_BLOCKED', 100, 'RUB', 1)`, nil},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.Exec(tc.query, tc.args...); err == nil {
				t.Fatal("ожидалась ошибка CHECK-ограничения")
			}
		})
	}

	app := insertApp(t, db, limitservice.ScoringApproved)
	opInsert := `INSERT INTO limit_block_operation (application_id, operation_id, lender_id, amount_minor, currency, state, block_id)
		VALUES ($1, gen_random_uuid(), $2, 100, 'RUB', $3, $4)`
	if _, err := db.Exec(opInsert, app.ID, app.LenderID, "CONFIRMED", ""); err == nil {
		t.Error("CONFIRMED без block_id должен отклоняться")
	}
	if _, err := db.Exec(opInsert, app.ID, app.LenderID, "PENDING", "blk-1"); err == nil {
		t.Error("PENDING с block_id должен отклоняться")
	}
}

// --- FindByID ---

func TestFindByID(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)

	got, err := r.FindByID(context.Background(), app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != app {
		t.Errorf("прочитано %+v, вставлено %+v", got, app)
	}

	if _, err := r.FindByID(context.Background(), uuid.NewString()); !errors.Is(err, limitservice.ErrNotFound) {
		t.Errorf("несуществующая заявка: %v, ожидалась ErrNotFound", err)
	}
}

// --- InsertBlockOperation ---

func TestInsertBlockOperation_CreatesOnceAndReturnsStored(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)

	first, err := r.InsertBlockOperation(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	want := limitservice.BlockRequest{
		OperationID:   first.OperationID,
		ApplicationID: app.ID,
		LenderID:      app.LenderID,
		AmountMinor:   app.RequestedAmountMinor,
		Currency:      app.Currency,
	}
	if first.BlockRequest != want || first.State != limitservice.OperationPending || first.BlockId != "" {
		t.Errorf("операция %+v, ожидалась PENDING с параметрами %+v", first, want)
	}
	if _, err := uuid.Parse(first.OperationID); err != nil {
		t.Errorf("OperationID не UUID: %q", first.OperationID)
	}

	// Повтор — даже с изменённой суммой в заявке — возвращает сохранённую операцию.
	changed := app
	changed.RequestedAmountMinor = 1
	second, err := r.InsertBlockOperation(context.Background(), changed)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Errorf("повтор вернул %+v, ожидалась сохранённая %+v", second, first)
	}

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM limit_block_operation`).Scan(&n); err != nil || n != 1 {
		t.Errorf("операций в таблице: %d (%v), ожидалась 1", n, err)
	}
}

// Одновременные вызовы по одной заявке получают один и тот же OperationID.
func TestInsertBlockOperation_Concurrent(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)

	const n = 10
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			op, err := r.InsertBlockOperation(context.Background(), app)
			ids[i], errs[i] = op.OperationID, err
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range ids {
		if errs[i] != nil {
			t.Fatalf("вызов %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("разные OperationID: %s и %s", ids[0], ids[i])
		}
	}
}

// --- Outbox ---

func TestOutbox_Success(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)
	op, err := r.InsertBlockOperation(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	op.BlockId = "blk-42"
	msg := message(app.ID)

	got, err := r.Outbox(context.Background(), op, msg)
	if err != nil {
		t.Fatal(err)
	}

	stored := readApp(t, r, app.ID)
	if stored.Status != limitservice.LimitBlocked || stored.LenderBlockID != "blk-42" || stored.Version != app.Version+1 {
		t.Errorf("заявка: %+v", stored)
	}
	if got != stored {
		t.Errorf("метод вернул %+v, в БД %+v", got, stored)
	}
	if row, _ := readOp(t, db, app.ID); row.State != "CONFIRMED" || row.BlockID != "blk-42" {
		t.Errorf("операция: %+v", row)
	}

	var eventID, topic, key, payload string
	err = db.QueryRow(`SELECT event_id, topic, msg_key, payload::text FROM outbox_events`).Scan(&eventID, &topic, &key, &payload)
	if err != nil {
		t.Fatalf("событие: %v", err)
	}
	if eventID != msg.EventID || topic != msg.Topic || key != msg.Key || payload != string(msg.Payload) {
		t.Errorf("событие (%s, %s, %s, %s), ожидалось %+v", eventID, topic, key, payload, msg)
	}
}

// Заявка уже не SCORING_APPROVED: ErrConflict, ничего не меняется.
func TestOutbox_ApplicationNotApproved(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)
	op, err := r.InsertBlockOperation(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE applications SET status = 'REJECTED', version = version + 1 WHERE id = $1`, app.ID); err != nil {
		t.Fatal(err)
	}
	before := readApp(t, r, app.ID)

	op.BlockId = "blk-1"
	_, err = r.Outbox(context.Background(), op, message(app.ID))
	if !errors.Is(err, limitservice.ErrConflict) {
		t.Fatalf("ошибка %v, ожидалась ErrConflict", err)
	}
	if after := readApp(t, r, app.ID); after != before {
		t.Errorf("заявка изменилась: %+v", after)
	}
	if row, _ := readOp(t, db, app.ID); row.State != "PENDING" || row.BlockID != "" {
		t.Errorf("операция изменилась: %+v", row)
	}
	if n := countOutbox(t, db); n != 0 {
		t.Errorf("событий: %d, ожидалось 0", n)
	}
}

// Повторный Outbox после успеха: второго события нет.
func TestOutbox_SecondCallNoDuplicateEvent(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)
	op, _ := r.InsertBlockOperation(context.Background(), app)
	op.BlockId = "blk-1"

	if _, err := r.Outbox(context.Background(), op, message(app.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Outbox(context.Background(), op, message(app.ID)); !errors.Is(err, limitservice.ErrConflict) {
		t.Fatalf("второй вызов: %v, ожидалась ErrConflict", err)
	}
	if n := countOutbox(t, db); n != 1 {
		t.Errorf("событий: %d, ожидалось 1", n)
	}
}

// Атомарность: заявка уже обновлена внутри транзакции, но операция не PENDING —
// транзакция откатывается целиком, заявка остаётся SCORING_APPROVED.
func TestOutbox_RollbackWhenOperationNotPending(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)
	op, _ := r.InsertBlockOperation(context.Background(), app)
	if err := r.MarkDeclined(context.Background(), app.ID, op.OperationID); err != nil {
		t.Fatal(err)
	}

	op.BlockId = "blk-1"
	_, err := r.Outbox(context.Background(), op, message(app.ID))
	if !errors.Is(err, limitservice.ErrConflict) {
		t.Fatalf("ошибка %v, ожидалась ErrConflict", err)
	}
	if got := readApp(t, r, app.ID); got != app {
		t.Errorf("заявка не откатилась: %+v", got)
	}
	if n := countOutbox(t, db); n != 0 {
		t.Errorf("событий: %d, ожидалось 0", n)
	}
}

// Атомарность: вставка события упала (дубль event_id) — откатываются и заявка,
// и операция. Статус без события не сохраняется.
func TestOutbox_RollbackWhenOutboxInsertFails(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	app := insertApp(t, db, limitservice.ScoringApproved)
	op, _ := r.InsertBlockOperation(context.Background(), app)

	msg := message(app.ID)
	if _, err := db.Exec(`INSERT INTO outbox_events (event_id, topic, msg_key, payload) VALUES ($1, 't', 'k', '{}')`, msg.EventID); err != nil {
		t.Fatal(err)
	}

	op.BlockId = "blk-1"
	if _, err := r.Outbox(context.Background(), op, msg); err == nil {
		t.Fatal("ожидалась ошибка вставки события")
	}
	if got := readApp(t, r, app.ID); got != app {
		t.Errorf("заявка не откатилась: %+v", got)
	}
	if row, _ := readOp(t, db, app.ID); row.State != "PENDING" || row.BlockID != "" {
		t.Errorf("операция не откатилась: %+v", row)
	}
	if n := countOutbox(t, db); n != 1 {
		t.Errorf("событий: %d, ожидалось 1 (только подготовленное тестом)", n)
	}
}

// --- MarkDeclined ---

func TestMarkDeclined(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	ctx := context.Background()

	pending := insertApp(t, db, limitservice.ScoringApproved)
	op, _ := r.InsertBlockOperation(ctx, pending)

	// Чужой operation_id — ничего не меняется.
	if err := r.MarkDeclined(ctx, pending.ID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if row, _ := readOp(t, db, pending.ID); row.State != "PENDING" {
		t.Errorf("чужой operation_id изменил операцию: %+v", row)
	}

	if err := r.MarkDeclined(ctx, pending.ID, op.OperationID); err != nil {
		t.Fatal(err)
	}
	if row, _ := readOp(t, db, pending.ID); row.State != "DECLINED" {
		t.Errorf("операция %+v, ожидалась DECLINED", row)
	}
	if got := readApp(t, r, pending.ID); got != pending {
		t.Errorf("MarkDeclined изменил заявку: %+v", got)
	}

	// Подтверждённую операцию отказ не перезаписывает.
	confirmed := insertApp(t, db, limitservice.ScoringApproved)
	cop, _ := r.InsertBlockOperation(ctx, confirmed)
	cop.BlockId = "blk-1"
	if _, err := r.Outbox(ctx, cop, message(confirmed.ID)); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkDeclined(ctx, confirmed.ID, cop.OperationID); err != nil {
		t.Fatal(err)
	}
	if row, _ := readOp(t, db, confirmed.ID); row.State != "CONFIRMED" || row.BlockID != "blk-1" {
		t.Errorf("CONFIRMED перезаписан: %+v", row)
	}
}

// --- FindPendingOperations ---

func TestFindPendingOperations(t *testing.T) {
	db := testDB(t)
	r := newRepo(t, db)
	ctx := context.Background()

	newOp := func(age time.Duration) limitservice.LimitOperation {
		app := insertApp(t, db, limitservice.ScoringApproved)
		op, err := r.InsertBlockOperation(ctx, app)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE limit_block_operation SET updated_at = now() - make_interval(secs => $2) WHERE application_id = $1`,
			app.ID, age.Seconds()); err != nil {
			t.Fatal(err)
		}
		return op
	}

	older := newOp(10 * time.Minute)
	old := newOp(5 * time.Minute)
	_ = newOp(10 * time.Second) // свежая — ещё может обрабатываться обычным вызовом
	declined := newOp(10 * time.Minute)
	if err := r.MarkDeclined(ctx, declined.ApplicationID, declined.OperationID); err != nil {
		t.Fatal(err)
	}

	ops, err := r.FindPendingOperations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 2 {
		t.Fatalf("найдено %d операций, ожидалось 2 (старые PENDING): %+v", len(ops), ops)
	}
	if ops[0] != older || ops[1] != old {
		t.Errorf("порядок/содержимое: %+v, ожидалось сначала самая старая %s, затем %s",
			ops, older.OperationID, old.OperationID)
	}
}
