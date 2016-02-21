#!/bin/sh
set -e
runhaskell -- -I0 Build.hs "$@"
