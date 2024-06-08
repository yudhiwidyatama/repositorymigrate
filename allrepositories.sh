#!/bin/bash
oc get is -o go-template='{{range .items}}{{$ns := .metadata.namespace}}{{$nm := .metadata.name}}{{range .status.tags}}{{$tag := .tag}}{{ range .items}}{{$ns}}|{{$nm}}|{{$tag}}|{{.image}}{{"\n"}}{{end}}{{end}}{{end}}' --all-namespaces
