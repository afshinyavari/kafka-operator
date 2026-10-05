{{/*
Labels operatorn sätter på app-podden. Används som selector för proxyns
Service och NetworkPolicy.
*/}}
{{- define "registry.appPodSelector" -}}
app.kubernetes.io/name: apicurio-registry
app.kubernetes.io/component: app
app.kubernetes.io/instance: {{ .Values.name }}
{{- end -}}

{{/*
commonName i proxyns Kafka-klientcertifikat. Blir namnet på proxyns KafkaUser,
eftersom Strimzi lägger ACL:erna på User:CN=<namnet på KafkaUser>.
*/}}
{{- define "registry.proxyKafkaCommonName" -}}
{{- .Values.proxy.kafkaCommonName | default (printf "%s-proxy-kafka" .Values.name) -}}
{{- end -}}

{{/*
Namnet på Kafka-topicen för en roll (journal/events/snapshots).

Topicen under journal ÄR registryts databas när storage.type=kafkasql. Utan
angivet värde gäller den gamla regeln, _schemas, så att en befintlig
installation fortsätter läsa sin egen databas efter en chartuppgradering.
Ange topicName bara för en installation vars topic heter något annat — och
aldrig på en som redan kör, då startar registryt mot en tom journal.

Anrop: {{ include "registry.topicName" (list . "journal") }}
*/}}
{{- define "registry.topicName" -}}
{{- $root := index . 0 -}}
{{- $roll := index . 1 -}}
{{- $t := index ($root.Values.kafka.topics | default dict) $roll | default dict -}}
{{- if $t.topicName -}}
{{- $t.topicName -}}
{{- else -}}
{{- printf "%s-%s" $root.Values.name $roll -}}
{{- end -}}
{{- end -}}

{{/*
Namnet på KafkaTopic-objektet för en roll. Skiljs från topicName eftersom Kafka
tillåter tecken som Kubernetes inte gör — understrecket i _schemas ger objektet
det ogiltiga namnet -schemas om det inte prefixas.

Utan angivet värde gäller den gamla regeln: namn + topicnamnet med understreck
utbytta mot bindestreck, vilket ger registry-test-schemas för _schemas. Topics
vars namn inte börjar med understreck används som de är. Resultatet gemeniseras
eftersom Kafka tillåter versaler i topicnamn men Kubernetes inte i objektnamn.
*/}}
{{- define "registry.topicResourceName" -}}
{{- $root := index . 0 -}}
{{- $roll := index . 1 -}}
{{- $t := index ($root.Values.kafka.topics | default dict) $roll | default dict -}}
{{- if $t.resourceName -}}
{{- $t.resourceName | lower -}}
{{- else -}}
{{- $namn := include "registry.topicName" (list $root $roll) -}}
{{- if hasPrefix "_" $namn -}}
{{- printf "%s%s" $root.Values.name (replace "_" "-" $namn) | lower -}}
{{- else -}}
{{- $namn | replace "_" "-" | lower -}}
{{- end -}}
{{- end -}}
{{- end -}}
