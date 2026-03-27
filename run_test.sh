#!/bin/bash
set -eu
go test ./... | grep -v `date +%Y-%m-%d`
