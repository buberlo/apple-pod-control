package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/buberlo/apple-pod-control/internal/model"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if path != ":memory:" {
		path, _ = filepath.Abs(path)
	}
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS deployments (
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  document BLOB NOT NULL,
  generation INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(namespace, name)
);
CREATE TABLE IF NOT EXISTS nodes (
  id TEXT PRIMARY KEY,
  hostname TEXT NOT NULL,
  address TEXT NOT NULL,
  architecture TEXT NOT NULL,
  cpu_count INTEGER NOT NULL,
  memory_bytes INTEGER NOT NULL,
  labels BLOB NOT NULL,
  runtime_version TEXT NOT NULL,
  state TEXT NOT NULL,
  last_seen TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS workloads (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  deployment TEXT NOT NULL,
  generation INTEGER NOT NULL,
  revision TEXT NOT NULL,
  replica INTEGER NOT NULL,
  node_id TEXT NOT NULL DEFAULT '',
  container_name TEXT NOT NULL UNIQUE,
  labels BLOB NOT NULL,
  state TEXT NOT NULL,
  ready INTEGER NOT NULL DEFAULT 0,
  message TEXT NOT NULL DEFAULT '',
  address TEXT NOT NULL DEFAULT '',
  restart_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS workloads_deployment_idx ON workloads(namespace, deployment, generation);
CREATE INDEX IF NOT EXISTS workloads_node_idx ON workloads(node_id, state);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("migrate sqlite: %w", err)
	}
	if err := s.ensureColumn(ctx, "workloads", "revision", `ALTER TABLE workloads ADD COLUMN revision TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, statement string) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return fmt.Errorf("inspect sqlite schema: %w", err)
	}
	found := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan sqlite schema: %w", err)
		}
		if name == column {
			found = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("add sqlite column %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) UpsertDeployment(ctx context.Context, input model.Deployment) (model.Deployment, bool, error) {
	now := time.Now().UTC()
	if err := input.DefaultAndValidate(); err != nil {
		return model.Deployment{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Deployment{}, false, fmt.Errorf("begin deployment upsert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var oldDocument []byte
	var generation int64
	var createdText string
	err = tx.QueryRowContext(ctx, `SELECT document, generation, created_at FROM deployments WHERE namespace = ? AND name = ?`, input.Metadata.Namespace, input.Metadata.Name).
		Scan(&oldDocument, &generation, &createdText)
	created := errors.Is(err, sql.ErrNoRows)
	if err != nil && !created {
		return model.Deployment{}, false, fmt.Errorf("read deployment: %w", err)
	}

	createdAt := now
	if created {
		generation = 1
		input.Metadata.UID = uuid.NewString()
	} else {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, createdText); parseErr == nil {
			createdAt = parsed
		}
		var previous model.Deployment
		if err := json.Unmarshal(oldDocument, &previous); err != nil {
			return model.Deployment{}, false, fmt.Errorf("decode stored deployment: %w", err)
		}
		input.Metadata.UID = previous.Metadata.UID
		oldSpec, _ := json.Marshal(previous.Spec)
		newSpec, _ := json.Marshal(input.Spec)
		if string(oldSpec) != string(newSpec) {
			generation++
		}
	}

	input.Metadata.Generation = generation
	input.Metadata.ResourceVersion = fmt.Sprintf("%d", now.UnixNano())
	input.Metadata.CreatedAt = createdAt
	input.Metadata.UpdatedAt = now
	input.Status = model.DeploymentStatus{}
	document, err := json.Marshal(input)
	if err != nil {
		return model.Deployment{}, false, fmt.Errorf("encode deployment: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO deployments(namespace, name, document, generation, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(namespace, name) DO UPDATE SET document=excluded.document, generation=excluded.generation, updated_at=excluded.updated_at`,
		input.Metadata.Namespace, input.Metadata.Name, document, generation, createdAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return model.Deployment{}, false, fmt.Errorf("write deployment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.Deployment{}, false, fmt.Errorf("commit deployment: %w", err)
	}
	return input, created, nil
}

func (s *Store) GetDeployment(ctx context.Context, namespace, name string) (model.Deployment, error) {
	var document []byte
	if err := s.db.QueryRowContext(ctx, `SELECT document FROM deployments WHERE namespace = ? AND name = ?`, namespace, name).Scan(&document); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Deployment{}, ErrNotFound
		}
		return model.Deployment{}, fmt.Errorf("get deployment: %w", err)
	}
	var deployment model.Deployment
	if err := json.Unmarshal(document, &deployment); err != nil {
		return model.Deployment{}, fmt.Errorf("decode deployment: %w", err)
	}
	return deployment, nil
}

func (s *Store) ListDeployments(ctx context.Context, namespace string) ([]model.Deployment, error) {
	query := `SELECT document FROM deployments`
	var args []any
	if namespace != "" {
		query += ` WHERE namespace = ?`
		args = append(args, namespace)
	}
	query += ` ORDER BY namespace, name`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	defer rows.Close()
	var deployments []model.Deployment
	for rows.Next() {
		var document []byte
		if err := rows.Scan(&document); err != nil {
			return nil, fmt.Errorf("scan deployment: %w", err)
		}
		var deployment model.Deployment
		if err := json.Unmarshal(document, &deployment); err != nil {
			return nil, fmt.Errorf("decode deployment: %w", err)
		}
		deployments = append(deployments, deployment)
	}
	return deployments, rows.Err()
}

func (s *Store) DeleteDeployment(ctx context.Context, namespace, name string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM deployments WHERE namespace = ? AND name = ?`, namespace, name)
	if err != nil {
		return fmt.Errorf("delete deployment: %w", err)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpsertNode(ctx context.Context, node model.Node) error {
	labels, err := json.Marshal(node.Labels)
	if err != nil {
		return fmt.Errorf("encode node labels: %w", err)
	}
	if node.LastSeen.IsZero() {
		node.LastSeen = time.Now().UTC()
	}
	if node.State == "" {
		node.State = "Ready"
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO nodes(id, hostname, address, architecture, cpu_count, memory_bytes, labels, runtime_version, state, last_seen)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET hostname=excluded.hostname, address=excluded.address,
architecture=excluded.architecture, cpu_count=excluded.cpu_count, memory_bytes=excluded.memory_bytes,
labels=excluded.labels, runtime_version=excluded.runtime_version, state=excluded.state, last_seen=excluded.last_seen`,
		node.ID, node.Hostname, node.Address, node.Architecture, node.CPUCount, node.MemoryBytes, labels,
		node.RuntimeVersion, node.State, node.LastSeen.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}
	return nil
}

func (s *Store) TouchNode(ctx context.Context, id string, at time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE nodes SET last_seen = ?, state = 'Ready' WHERE id = ?`, at.UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return fmt.Errorf("touch node: %w", err)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ListNodes(ctx context.Context) ([]model.Node, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, hostname, address, architecture, cpu_count, memory_bytes, labels, runtime_version, state, last_seen FROM nodes ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()
	var nodes []model.Node
	for rows.Next() {
		var node model.Node
		var labels []byte
		var lastSeen string
		if err := rows.Scan(&node.ID, &node.Hostname, &node.Address, &node.Architecture, &node.CPUCount,
			&node.MemoryBytes, &labels, &node.RuntimeVersion, &node.State, &lastSeen); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		_ = json.Unmarshal(labels, &node.Labels)
		node.LastSeen, _ = time.Parse(time.RFC3339Nano, lastSeen)
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}

func (s *Store) MarkStaleNodes(ctx context.Context, cutoff time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE nodes SET state = 'NotReady' WHERE last_seen < ? AND state != 'NotReady'`, cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("mark stale nodes: %w", err)
	}
	return nil
}

func (s *Store) MarkNodeWorkloadsUnknown(ctx context.Context, nodeID, message string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE workloads
SET state = 'Unknown', ready = 0, message = ?, updated_at = ?
WHERE node_id = ? AND state NOT IN ('Stopping', 'Failed')`,
		message, time.Now().UTC().Format(time.RFC3339Nano), nodeID)
	if err != nil {
		return fmt.Errorf("mark node workloads unknown: %w", err)
	}
	return nil
}

func (s *Store) CreateWorkload(ctx context.Context, workload model.Workload) error {
	now := time.Now().UTC()
	if workload.CreatedAt.IsZero() {
		workload.CreatedAt = now
	}
	workload.UpdatedAt = now
	labels, err := json.Marshal(workload.Labels)
	if err != nil {
		return fmt.Errorf("encode workload labels: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO workloads(id, namespace, deployment, generation, revision, replica, node_id, container_name, labels, state, ready, message, address, restart_count, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, workload.ID, workload.Namespace, workload.Deployment, workload.Generation, workload.Revision,
		workload.Replica, workload.NodeID, workload.ContainerName, labels, workload.State, workload.Ready, workload.Message,
		workload.Address, workload.RestartCount, workload.CreatedAt.Format(time.RFC3339Nano), workload.UpdatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("create workload: %w", err)
	}
	return nil
}

func (s *Store) AssignWorkload(ctx context.Context, id, nodeID string) error {
	return s.updateWorkload(ctx, id, `node_id = ?, state = 'Assigned', message = ''`, nodeID)
}

func (s *Store) UnassignWorkload(ctx context.Context, id, message string) error {
	return s.updateWorkload(ctx, id, `node_id = '', state = 'Pending', ready = 0, message = ?`, message)
}

func (s *Store) BackoffWorkload(ctx context.Context, id, message string) error {
	return s.updateWorkload(ctx, id, `node_id = '', state = 'Backoff', ready = 0, message = ?`, message)
}

func (s *Store) UpdateWorkloadObservation(ctx context.Context, id, state string, ready bool, message, address string, restartCount int) error {
	return s.updateWorkload(ctx, id, `state = ?, ready = ?, message = ?, address = ?, restart_count = ?`, state, ready, message, address, restartCount)
}

func (s *Store) SetWorkloadState(ctx context.Context, id, state, message string) error {
	return s.updateWorkload(ctx, id, `state = ?, message = ?`, state, message)
}

func (s *Store) updateWorkload(ctx context.Context, id, setClause string, values ...any) error {
	values = append(values, time.Now().UTC().Format(time.RFC3339Nano), id)
	result, err := s.db.ExecContext(ctx, `UPDATE workloads SET `+setClause+`, updated_at = ? WHERE id = ?`, values...)
	if err != nil {
		return fmt.Errorf("update workload: %w", err)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteWorkload(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM workloads WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete workload: %w", err)
	}
	return nil
}

func (s *Store) ListWorkloads(ctx context.Context) ([]model.Workload, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, namespace, deployment, generation, revision, replica, node_id, container_name, labels, state, ready, message, address, restart_count, created_at, updated_at
FROM workloads ORDER BY namespace, deployment, generation, replica`)
	if err != nil {
		return nil, fmt.Errorf("list workloads: %w", err)
	}
	defer rows.Close()
	var workloads []model.Workload
	for rows.Next() {
		var workload model.Workload
		var labels []byte
		var createdAt, updatedAt string
		if err := rows.Scan(&workload.ID, &workload.Namespace, &workload.Deployment, &workload.Generation, &workload.Revision, &workload.Replica,
			&workload.NodeID, &workload.ContainerName, &labels, &workload.State, &workload.Ready, &workload.Message,
			&workload.Address, &workload.RestartCount, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan workload: %w", err)
		}
		_ = json.Unmarshal(labels, &workload.Labels)
		workload.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		workload.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
		workloads = append(workloads, workload)
	}
	return workloads, rows.Err()
}
