#!/bin/sh

createuser -U postgres -s "$HOST_USER" 2>/dev/null || true
