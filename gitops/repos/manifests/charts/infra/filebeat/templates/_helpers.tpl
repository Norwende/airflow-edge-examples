{{/* Nom des ressources : filebeat-<env> (Beat, ServiceAccount, ClusterRole...). */}}
{{- define "filebeat.name" -}}
{{- printf "filebeat-%s" .Values.env }}
{{- end }}

{{/* Namespace applicatif collecté (et namespace de l'Elasticsearch). */}}
{{- define "filebeat.sourceNamespace" -}}
{{- printf "gitops-%s" .Values.env }}
{{- end }}

{{- define "filebeat.labels" -}}
app.kubernetes.io/name: filebeat
app.kubernetes.io/instance: {{ include "filebeat.name" . }}
app.kubernetes.io/part-of: gitops-lab
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
gitops.lab/env: {{ .Values.env }}
{{- end }}
