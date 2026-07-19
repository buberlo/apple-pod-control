{{- define "apc-web.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "apc-web.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "apc-web.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "apc-web.labels" -}}
app.kubernetes.io/name: {{ include "apc-web.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}
