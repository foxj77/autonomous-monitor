{{/*
Expand the name of the chart.
*/}}
{{- define "autonomous-monitor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated to 63 characters because Kubernetes name fields have this limit.
If the release name already contains the chart name, avoid duplicating it.
*/}}
{{- define "autonomous-monitor.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label value: "<name>-<version>" with "+" replaced by "_".
*/}}
{{- define "autonomous-monitor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "autonomous-monitor.labels" -}}
helm.sh/chart: {{ include "autonomous-monitor.chart" . }}
{{ include "autonomous-monitor.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels — used by the Deployment selector, Service selector, and
NetworkPolicy podSelector. These must never change after initial install;
Kubernetes rejects selector mutations on existing Deployments.
*/}}
{{- define "autonomous-monitor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "autonomous-monitor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Fully-qualified image reference.
Tag falls back to .Chart.AppVersion when values.image.tag is empty,
so a plain `helm upgrade` always uses the version the chart was built for.
*/}}
{{- define "autonomous-monitor.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*
ServiceAccount name. Defaults to "<fullname>" unless explicitly overridden.
*/}}
{{- define "autonomous-monitor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "autonomous-monitor.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Namespace the monitor watches. Defaults to the release namespace so that
per-namespace installs Just Work without a per-overlay patch.
*/}}
{{- define "autonomous-monitor.watchNamespace" -}}
{{- .Values.watch.namespace | default .Release.Namespace }}
{{- end }}
