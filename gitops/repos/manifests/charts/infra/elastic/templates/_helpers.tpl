{{/* Labels communs du chart elastic. */}}
{{- define "elastic.labels" -}}
app.kubernetes.io/part-of: gitops-lab
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
gitops.lab/env: {{ .Values.env }}
{{- end }}

{{/* Service HTTP créé par ECK pour Kibana. */}}
{{- define "elastic.kibanaService" -}}
{{- printf "%s-kb-http" .Values.kibana.name }}
{{- end }}

{{/* Secret du superutilisateur elastic créé par ECK. */}}
{{- define "elastic.elasticUserSecret" -}}
{{- printf "%s-es-elastic-user" .Values.elasticsearch.name }}
{{- end }}
