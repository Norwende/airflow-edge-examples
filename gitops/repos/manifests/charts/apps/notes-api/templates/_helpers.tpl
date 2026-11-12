{{/* Nom court du chart. */}}
{{- define "notes-api.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Nom complet : avec releaseName "notes-api" (ApplicationSet), vaut "notes-api". */}}
{{- define "notes-api.fullname" -}}
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

{{- define "notes-api.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels de sélection (immuables une fois le Deployment créé). */}}
{{- define "notes-api.selectorLabels" -}}
app.kubernetes.io/name: {{ include "notes-api.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Labels communs. */}}
{{- define "notes-api.labels" -}}
helm.sh/chart: {{ include "notes-api.chart" . }}
{{ include "notes-api.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | trunc 63 | trimSuffix "-" | quote }}
app.kubernetes.io/component: api
app.kubernetes.io/part-of: gitops-lab
app.kubernetes.io/managed-by: {{ .Release.Service }}
gitops.lab/env: {{ .Values.env }}
{{- end }}

{{- define "notes-api.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "notes-api.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
