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

{{/* The licence and the Enterprise Edition's secrets (OIDC client secret, LDAP bind password, SCIM token). */}}
{{- define "xmsg.enterpriseSecretName" -}}
{{- default (printf "%s-enterprise" (include "xmsg.fullname" .)) .Values.enterprise.existingSecret -}}
{{- end -}}

{{- define "xmsg.natsUrl" -}}
{{- if .Values.nats.bundled -}}nats://{{ include "xmsg.fullname" . }}-nats:4222{{- else -}}{{ required "nats.url is required when nats.bundled is false" .Values.nats.url }}{{- end -}}
{{- end -}}

{{- define "xmsg.valkeyUrl" -}}
{{- if .Values.valkey.bundled -}}redis://{{ include "xmsg.fullname" . }}-valkey:6379{{- else -}}{{ .Values.valkey.url }}{{- end -}}
{{- end -}}

{{/*
The Rust services: name, binary and HTTP port (0 = a worker without one). Optional ones
are left out when their backend isn't configured. With `enterprise.enabled`, auth runs the
Enterprise Edition binary from the enterprise image, the admin API runs, and SCIM can
be added.
*/}}
{{- define "xmsg.components" -}}
{{- $auth := dict "name" "auth" "bin" "xmsg-auth" "port" 8081 "portEnv" "XMSG_AUTH_PORT" -}}
{{- if .Values.enterprise.enabled -}}
{{- $auth = dict "name" "auth" "bin" "xmsg-auth-ee" "port" 8081 "portEnv" "XMSG_AUTH_PORT" "enterprise" true -}}
{{- end -}}
{{- $c := list
  $auth
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
{{- if .Values.enterprise.enabled -}}{{- $c = append $c (dict "name" "admin" "bin" "xmsg-admin-ee" "port" 8096 "portEnv" "XMSG_EE_ADMIN_PORT" "enterprise" true) -}}{{- end -}}
{{- if and .Values.enterprise.enabled .Values.enterprise.scim.enabled -}}{{- $c = append $c (dict "name" "scim" "bin" "xmsg-scim-ee" "port" 8095 "portEnv" "XMSG_EE_SCIM_PORT" "enterprise" true) -}}{{- end -}}
{{- if .Values.opensearch.url -}}{{- $c = append $c (dict "name" "search-indexer" "bin" "xmsg-search-indexer" "port" 0) -}}{{- end -}}
{{- if .Values.media.enabled -}}{{- $c = append $c (dict "name" "media" "bin" "xmsg-media" "port" 8087 "portEnv" "XMSG_MEDIA_PORT") -}}{{- end -}}
{{- toJson $c -}}
{{- end -}}

{{/* The bundled Garage runs when media is on and no S3 endpoint of your own is set. */}}
{{- define "xmsg.garageBundled" -}}
{{- if and .Values.media.enabled (not .Values.media.s3.endpoint) -}}true{{- end -}}
{{- end -}}
