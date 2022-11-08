ID=$(echo $HOSTNAME | rev | cut -d- -f1 | rev)
cat >/home/denzel/work/projects/golog/config.yaml <<EOD
data-dir: /var/run/proglog/data
rpc-port: {{.Values.rpcPort}}
bind-addr: \
  "$HOSTNAME.proglog.{{.Release.Namespace}}.\svc.cluster.local:\
    {{.Values.serfPort}}"
$([ $ID != 0 ] && echo 'start-join-addrs: \
  "proglog-0.proglog.{{.Release.Namespace}}.svc.cluster.local:\
    {{.Values.serfPort}}"')
bootstrap: $([ $ID = 0 ] && echo true || echo false )
EOD