#!/bin/sh

# The postgres role already exists; only create a role when the host
# user is a distinct name, and let real failures abort initialization.
if [ -n "${HOST_USER}" ] && [ "${HOST_USER}" != "postgres" ]; then
	createuser -U postgres -s "${HOST_USER}"
fi
