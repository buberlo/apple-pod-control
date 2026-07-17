package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/buberlo/apple-pod-control/internal/control"
	"github.com/buberlo/apple-pod-control/internal/model"
	"github.com/buberlo/apple-pod-control/internal/store"
)

type Server struct {
	store      *store.Store
	reconciler *control.Reconciler
	logger     *slog.Logger
	token      string
	startedAt  time.Time
}

func NewServer(database *store.Store, reconciler *control.Reconciler, logger *slog.Logger, bearerToken string) *Server {
	return &Server{store: database, reconciler: reconciler, logger: logger, token: bearerToken, startedAt: time.Now()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /version", s.version)
	mux.HandleFunc("/apis/apc.dev/v1alpha1/", s.resources)
	mux.HandleFunc("/api/v1/", s.coreResources)
	return s.logging(s.authenticate(mux))
}

func (s *Server) version(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"gitVersion": "v0.1.0", "apiVersion": model.APIVersion, "uptimeSeconds": int(time.Since(s.startedAt).Seconds())})
}

func (s *Server) resources(writer http.ResponseWriter, request *http.Request) {
	parts := splitPath(request.URL.Path)
	if len(parts) < 6 || parts[0] != "apis" || parts[1] != "apc.dev" || parts[2] != "v1alpha1" || parts[3] != "namespaces" || parts[5] != "deployments" {
		writeStatus(writer, http.StatusNotFound, "NotFound", "resource not found")
		return
	}
	namespace := parts[4]
	name := ""
	if len(parts) > 6 {
		name = parts[6]
	}
	switch request.Method {
	case http.MethodGet:
		if name == "" {
			s.listDeployments(writer, request, namespace)
		} else {
			s.getDeployment(writer, request, namespace, name)
		}
	case http.MethodPost, http.MethodPut:
		s.applyDeployment(writer, request, namespace, name)
	case http.MethodDelete:
		if name == "" {
			writeStatus(writer, http.StatusMethodNotAllowed, "MethodNotAllowed", "a deployment name is required")
			return
		}
		s.deleteDeployment(writer, request, namespace, name)
	default:
		writer.Header().Set("Allow", "GET, POST, PUT, DELETE")
		writeStatus(writer, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func (s *Server) coreResources(writer http.ResponseWriter, request *http.Request) {
	parts := splitPath(request.URL.Path)
	if request.Method != http.MethodGet {
		writeStatus(writer, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
		return
	}
	if len(parts) == 3 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "nodes" {
		nodes, err := s.store.ListNodes(request.Context())
		if err != nil {
			s.internalError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"apiVersion": "v1", "kind": "NodeList", "items": nodes})
		return
	}
	if len(parts) == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "namespaces" && parts[4] == "pods" {
		workloads, err := s.store.ListWorkloads(request.Context())
		if err != nil {
			s.internalError(writer, err)
			return
		}
		items := make([]model.Workload, 0, len(workloads))
		for _, workload := range workloads {
			if workload.Namespace == parts[3] {
				items = append(items, workload)
			}
		}
		writeJSON(writer, http.StatusOK, map[string]any{"apiVersion": "v1", "kind": "PodList", "items": items})
		return
	}
	writeStatus(writer, http.StatusNotFound, "NotFound", "resource not found")
}

func (s *Server) listDeployments(writer http.ResponseWriter, request *http.Request, namespace string) {
	deployments, err := s.store.ListDeployments(request.Context(), namespace)
	if err != nil {
		s.internalError(writer, err)
		return
	}
	workloads, err := s.store.ListWorkloads(request.Context())
	if err != nil {
		s.internalError(writer, err)
		return
	}
	for index := range deployments {
		deployments[index].Status = deploymentStatus(deployments[index], workloads)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"apiVersion": model.APIVersion, "kind": "DeploymentList", "items": deployments})
}

func (s *Server) getDeployment(writer http.ResponseWriter, request *http.Request, namespace, name string) {
	deployment, err := s.store.GetDeployment(request.Context(), namespace, name)
	if errors.Is(err, store.ErrNotFound) {
		writeStatus(writer, http.StatusNotFound, "NotFound", fmt.Sprintf("deployments %q not found", name))
		return
	}
	if err != nil {
		s.internalError(writer, err)
		return
	}
	workloads, err := s.store.ListWorkloads(request.Context())
	if err != nil {
		s.internalError(writer, err)
		return
	}
	deployment.Status = deploymentStatus(deployment, workloads)
	writeJSON(writer, http.StatusOK, deployment)
}

func (s *Server) applyDeployment(writer http.ResponseWriter, request *http.Request, namespace, pathName string) {
	defer request.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var deployment model.Deployment
	if err := decoder.Decode(&deployment); err != nil {
		writeStatus(writer, http.StatusBadRequest, "BadRequest", "invalid deployment: "+err.Error())
		return
	}
	if deployment.Metadata.Namespace != "" && deployment.Metadata.Namespace != namespace {
		writeStatus(writer, http.StatusBadRequest, "BadRequest", "metadata.namespace does not match request namespace")
		return
	}
	deployment.Metadata.Namespace = namespace
	if pathName != "" && deployment.Metadata.Name != pathName {
		writeStatus(writer, http.StatusBadRequest, "BadRequest", "metadata.name does not match request path")
		return
	}
	stored, created, err := s.store.UpsertDeployment(request.Context(), deployment)
	if err != nil {
		writeStatus(writer, http.StatusUnprocessableEntity, "Invalid", err.Error())
		return
	}
	s.reconciler.Wake()
	statusCode := http.StatusOK
	if created {
		statusCode = http.StatusCreated
	}
	writeJSON(writer, statusCode, stored)
}

func (s *Server) deleteDeployment(writer http.ResponseWriter, request *http.Request, namespace, name string) {
	if err := s.store.DeleteDeployment(request.Context(), namespace, name); errors.Is(err, store.ErrNotFound) {
		writeStatus(writer, http.StatusNotFound, "NotFound", fmt.Sprintf("deployments %q not found", name))
		return
	} else if err != nil {
		s.internalError(writer, err)
		return
	}
	s.reconciler.Wake()
	writeJSON(writer, http.StatusOK, map[string]any{"apiVersion": "v1", "kind": "Status", "status": "Success", "details": map[string]string{"kind": "deployments", "name": name}})
}

func deploymentStatus(deployment model.Deployment, workloads []model.Workload) model.DeploymentStatus {
	status := model.DeploymentStatus{ObservedGeneration: deployment.Metadata.Generation, Revision: deployment.TemplateRevision()}
	for _, workload := range workloads {
		if workload.Namespace != deployment.Metadata.Namespace || workload.Deployment != deployment.Metadata.Name || workload.State == "Stopping" {
			continue
		}
		status.Replicas++
		if workload.Revision == deployment.TemplateRevision() {
			status.UpdatedReplicas++
		}
		if workload.Ready {
			status.ReadyReplicas++
			status.AvailableReplicas++
		}
	}
	status.UnavailableReplicas = deployment.Spec.Replicas - status.AvailableReplicas
	if status.UnavailableReplicas < 0 {
		status.UnavailableReplicas = 0
	}
	now := time.Now().UTC()
	available := status.AvailableReplicas >= deployment.Spec.Replicas
	conditionStatus := "False"
	reason := "MinimumReplicasUnavailable"
	if available {
		conditionStatus = "True"
		reason = "MinimumReplicasAvailable"
	}
	status.Conditions = []model.Condition{
		{Type: "Available", Status: conditionStatus, Reason: reason, LastTransitionTime: now},
		{Type: "Progressing", Status: boolStatus(status.UpdatedReplicas < deployment.Spec.Replicas || !available), Reason: "Reconciling", LastTransitionTime: now},
	}
	return status
}

func boolStatus(value bool) string {
	if value {
		return "True"
	}
	return "False"
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthz" || request.URL.Path == "/readyz" || s.token == "" {
			next.ServeHTTP(writer, request)
			return
		}
		if request.Header.Get("Authorization") != "Bearer "+s.token {
			writeStatus(writer, http.StatusUnauthorized, "Unauthorized", "valid bearer token required")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		next.ServeHTTP(writer, request)
		s.logger.Debug("HTTP request", "method", request.Method, "path", request.URL.Path, "duration", time.Since(started))
	})
}

func (s *Server) internalError(writer http.ResponseWriter, err error) {
	s.logger.Error("API request failed", "error", err)
	writeStatus(writer, http.StatusInternalServerError, "InternalError", "internal server error")
}

func splitPath(path string) []string {
	return strings.Split(strings.Trim(path, "/"), "/")
}

func writeStatus(writer http.ResponseWriter, code int, reason, message string) {
	writeJSON(writer, code, map[string]any{"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": reason, "message": message, "code": code})
}

func writeJSON(writer http.ResponseWriter, code int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(code)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return
	}
}
