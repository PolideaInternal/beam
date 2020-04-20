#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euox pipefail

WORKING_DIR="$(pwd)"
MY_DIR="$(cd "$(dirname "$0")" && pwd)"
pushd "${MY_DIR}" &>/dev/null || exit 1

IMAGE_NAME=beam-site
CONTAINER_NAME=beam-site-c

function usage {
cat << EOF
usage: ${0} <command> [<args>]

These are  ${0} commands used in various situations:

    build               Prepare dist directory with landing pages and documentation
    develop             Starts the web server with preview of the website
    shell               Start shell
    build-image         Build a Docker image with a environment
    install-node-deps   Download all the Node dependencies
    check-links         Checks if the links are correct in the website
    cleanup             Delete the virtual environment in Docker
    stop                Stop the environment
    help                Display usage

Unrecognized commands are run as programs in the container.

For example, if you want to display a list of files, you
can execute the following command:

    $0 ls

The following command can also be performed from the Docker environment:
install-node-deps, preview, build-site, lint-css, lint-js.

The lint-css and lint-js accept paths in arguments. If no path is given, the script
will be executed for all supported files

EOF
}

function ensure_image_exists {
    if [[ ! $(docker images "${IMAGE_NAME}" -q) ]]; then
        echo "Image doesn't exist."
        build_image
    fi
}

function ensure_container_exists {
    if [[ ! $(docker container ls -a --filter="Name=${CONTAINER_NAME}" -q ) ]]; then
        echo "Container doesn't exist"
        docker run \
            --detach \
            --name "${CONTAINER_NAME}" \
            --volume "$(pwd):/opt/" \
            --publish 1313:1313 \
            "${IMAGE_NAME}" sh -c 'trap "exit 0" INT; while true; do sleep 30; done;'
        return 0
    fi
}

function ensure_container_running {
    container_status="$(docker inspect "${CONTAINER_NAME}" --format '{{.State.Status}}')"
    echo "Current container status: ${container_status}"
    if [[ ! "${container_status}" == "running" ]]; then
        echo "Container not running. Starting the container."
        docker start "${CONTAINER_NAME}"
    fi
}

function ensure_node_module_exists {
    if [[ ! -d website/www/node_modules/ ]] ; then
        echo "Missing node dependencies. Start installation."
        run_command "/opt/website/www/" yarn install
        echo "Dependencies installed."
    fi
}

function ensure_that_website_is_build {
    if [[ ! -f website/www/dist/index.html ]] ; then
        echo "The website is not built. Start building."
        run_command "/opt/website/www/" yarn build
        echo "The website has been built."
    fi
}

function build_image {
    echo "Start building image"
    docker build -f website/Dockerfile -t beam-site .
    echo "End building image"
}

function run_command {
    working_directory=$1
    shift
    if [[ -f /.dockerenv ]] ; then
        pushd "${working_directory}"
        exec "$@"
    else
        if ! test -t 0; then
            docker exec \
                --interactive \
                --workdir "${working_directory}" \
                "${CONTAINER_NAME}" "$@"
        else
            docker exec \
                --tty \
                --interactive \
                --workdir "${working_directory}" \
                "${CONTAINER_NAME}" "$@"
        fi
    fi
}

function prepare_environment {
    if [[ ! -f /.dockerenv ]] ; then
        ensure_image_exists
        ensure_container_exists
        ensure_container_running
    fi
}

function prevent_docker {
    if [[ -f /.dockerenv ]] ; then
        echo "This command is not supported in the Docker environment. Run this command from the host system."
        exit 1
    fi
}

function build_site {
    run_command "/opt/website/www/" yarn build
}

function cleanup_environment {
    container_status="$(docker inspect "${CONTAINER_NAME}" --format '{{.State.Status}}')"
    echo "Current container status: ${container_status}"
    if [[ "${container_status}" == "running" ]]; then
        echo "Container running. Killing the container."
        docker kill "${CONTAINER_NAME}"
    fi

    if [[ $(docker container ls -a --filter="Name=${CONTAINER_NAME}" -q ) ]]; then
        echo "Container exists. Removing the container."
        docker rm "${CONTAINER_NAME}"
    fi

    if [[ $(docker images "${IMAGE_NAME}" -q) ]]; then
        echo "Image exists. Deleting the image."
        docker rmi "${IMAGE_NAME}"
    fi
}

if [[ "$#" -eq 0 ]]; then
    echo "You must provide at least one command."
    echo
    usage
    exit 1
fi

CMD=$1

shift

# Check fundamentals commands
if [[ "${CMD}" == "build-image" ]] ; then
    prevent_docker
    build_image
    exit 0
elif [[ "${CMD}" == "stop" ]] ; then
    prevent_docker
    docker kill "${CONTAINER_NAME}"
    exit 0
elif [[ "${CMD}" == "cleanup" ]] ; then
    prevent_docker
    cleanup_environment
    exit 0
elif [[ "${CMD}" == "help" ]]; then
    usage
    exit 0
fi

prepare_environment

# Check container commands
if [[ "${CMD}" == "install-node-deps" ]] ; then
    run_command "/opt/website/www/" yarn install
elif [[ "${CMD}" == "develop" ]]; then
    ensure_node_module_exists
    run_command "/opt/website/www/" yarn develop --renderToDisk=true --bind="0.0.0.0" --baseURL="http://docker.local:1313"
elif [[ "${CMD}" == "build" ]]; then
    ensure_node_module_exists
    build_site
elif [[ "${CMD}" == "check-links" ]]; then
    ensure_node_module_exists
    ensure_that_website_is_build
    run_command "/opt/website/www/" ./check-links.sh
elif [[ "${CMD}" == "shell" ]]; then
    prevent_docker
    docker exec -ti "${CONTAINER_NAME}" bash
else
    prevent_docker
    docker exec -ti "${CONTAINER_NAME}" "${CMD}" "$@"
fi

popd &>/dev/null || exit 1
