#!/bin/bash
set -eu
(cd system && go test -v)
(cd app && go test -v)
