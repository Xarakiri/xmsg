{{/* SPDX-License-Identifier: AGPL-3.0-only */}}
{{- define "xmsg.fullname" -}}
{{- printf "%s-xmsg" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "xmsg.labels" -}}
app.kubernetes.io/part-of: xmsg
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "xmsg.image" -}}
{{- $ctx := index . 0 -}}{{- $name := index . 1 -}}
{{- printf "%s/%s:%s" $ctx.Values.image.registry $name (default $ctx.Chart.AppVersion $ctx.Values.image.tag) -}}
{{- end -}}

{{- define "xmsg.secretName" -}}
{{- default (printf "%s-secrets" (include "xmsg.fullname" .)) .Values.secrets.existingSecret -}}
{{- end -}}

{{- define "xmsg.natsUrl" -}}
{{- if .Values.nats.bundled -}}nats://{{ include "xmsg.fullname" . }}-nats:4222{{- else -}}{{ required "nats.url is required when nats.bundled is false" .Values.nats.url }}{{- end -}}
{{- end -}}

{{- define "xmsg.valkeyUrl" -}}
{{- if .Values.valkey.bundled -}}redis://{{ include "xmsg.fullname" . }}-valkey:6379{{- else -}}{{ .Values.valkey.url }}{{- end -}}
{{- end -}}

{{/*
The Rust services: name, binary and HTTP port (0 = a worker without one). Optional ones
are left out when their backend isn't configured.
*/}}
{{- define "xmsg.components" -}}
{{- $c := list
  (dict "name" "auth" "bin" "xmsg-auth" "port" 8081 "portEnv" "XMSG_AUTH_PORT")
  (dict "name" "users" "bin" "xmsg-users" "port" 8084 "portEnv" "XMSG_USERS_PORT")
  (dict "name" "channels" "bin" "xmsg-channels" "port" 8082 "portEnv" "XMSG_CHANNELS_PORT")
  (dict "name" "messages" "bin" "xmsg-messages" "port" 8083 "portEnv" "XMSG_MESSAGES_PORT")
  (dict "name" "readstate" "bin" "xmsg-readstate" "port" 8085 "portEnv" "XMSG_READSTATE_PORT")
  (dict "name" "persister" "bin" "xmsg-persister" "port" 0)
  (dict "name" "fanout" "bin" "xmsg-fanout" "port" 0)
  (dict "name" "outbox-relay" "bin" "xmsg-outbox-relay" "port" 0)
  (dict "name" "meta-relay" "bin" "xmsg-meta-relay" "port" 0)
  (dict "name" "reaction-batcher" "bin" "xmsg-reaction-batcher" "port" 0)
  (dict "name" "presence" "bin" "xmsg-presence" "port" 0)
-}}
{{- if .Values.push.enabled -}}{{- $c = append $c (dict "name" "push" "bin" "xmsg-push" "port" 8086 "portEnv" "XMSG_PUSH_PORT") -}}{{- end -}}
{{- if .Values.opensearch.url -}}{{- $c = append $c (dict "name" "search-indexer" "bin" "xmsg-search-indexer" "port" 0) -}}{{- end -}}
{{- toJson $c -}}
{{- end -}}
