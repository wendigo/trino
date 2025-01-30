#!/usr/bin/env bash

RED="\e[1;31m"
GREEN="\e[1;32m"
BLUE="\e[1;34m"
CC="\e[0m"

declare -A times

TRINO_COMMAND_EU=(./trino-cli-468.jar --server https://spooling-enabled.trino.galaxy.starburst.io --user 'mateusz.gajewski@starburstdata.com/accountadmin' --password)
TRINO_COMMAND_US=(./trino-cli-468.jar --server https://spooling-enabled-us.trino.galaxy.starburst.io --user 'mateusz.gajewski@starburstdata.com/accountadmin' --password)

function summary
{
    echo -e "${RED}EU latency:         ${GREEN}$(printf ' %7s' "${times['curl_eu']}")${CC} ms"
    echo -e "${BLUE}Without spooling${CC}:   ${RED}$(printf ' %7s' "${times['no_encoding_eu']}")${CC} ms"
    echo -e "${BLUE}json encoding${CC}:      ${GREEN}$(printf ' %7s' "${times['json_eu']}")${CC} ms"
    echo -e "${BLUE}json+zstd encoding${CC}: ${GREEN}$(printf ' %7s' "${times['json_zstd_eu']}")${CC} ms"
    echo -e "${BLUE}json+lz4 encoding${CC}:  ${GREEN}$(printf ' %7s' "${times['json_lz4_eu']}")${CC} ms"
    echo
    echo -e "${RED}US latency:         ${GREEN}$(printf ' %7s' "${times['curl_us']}")${CC} ms"
    echo -e "${BLUE}Without spooling${CC}:   ${RED}$(printf ' %7s' "${times['no_encoding_us']}")${CC} ms"
    echo -e "${BLUE}json encoding${CC}:      ${GREEN}$(printf ' %7s' "${times['json_us']}")${CC} ms"
    echo -e "${BLUE}json+zstd encoding${CC}: ${GREEN}$(printf ' %7s' "${times['json_zstd_us']}")${CC} ms"
    echo -e "${BLUE}json+lz4 encoding${CC}:  ${GREEN}$(printf ' %7s' "${times['json_lz4_us']}")${CC} ms"
}

function header
{
    echo -e "${GREEN}${1}${CC}"
    echo
}

function desc
{
    description="${1}"
    echo -e "${BLUE}${description}${CC}\n"
}

function run_command
{
    name="${1}"
    shift
    echo -n "$@"
    echo
    read -N1
    echo
    start_ms=$(ruby -e 'puts (Time.now.to_f * 1000).to_i')
    "$@"
    end_ms=$(ruby -e 'puts (Time.now.to_f * 1000).to_i')
    elapsed=$((end_ms-start_ms))
    times["${name}"]=$elapsed
    echo -ne "\n${GREEN}Command took: ${BLUE}${elapsed}${CC} ms\n\n"
}

function nextOrPrevious()
{
    example="${1}"
    next="example$((example + 1))"
    current="example${example}"
    previous="example$((example - 1))"

    while true; do
        stty -echo
        read -N1 key
        stty echo

        if [[ "${key}" = "n" || "${key}" = $'\n' ]]; then
            $next
        elif [[ "${key}" = "p" && "${example}" != "0" ]]; then
            $previous
        elif [[ "${key}" = "c" ]]; then
            $current
        fi
    done
}

function example0()
{
    clear
    header "Welcome to Trino Community Broadcast and spooling protocol demo!"
    desc "Let's ensure that clusters (EU & US) are up and running:"
    run_command "version_eu" ${TRINO_COMMAND_EU[@]} --execute 'SELECT version()'
    run_command "version_us" ${TRINO_COMMAND_US[@]} --execute 'SELECT version()'
    read
    desc "Now let's see the latency between the client and the servers:"
    echo -e "${RED}EU (Frankfurt) cluster${CC}"
    run_command "curl_eu" curl -w "@curl-format.txt" -o /dev/null -s https://spooling-enabled.trino.galaxy.starburst.io/v1/info
    echo -e "${RED}US (North Virginia) cluster${CC}"
    run_command "curl_us" curl -w "@curl-format.txt" -o /dev/null -s https://spooling-enabled-us.trino.galaxy.starburst.io/v1/info
    nextOrPrevious 0
}

function example1
{
    header "Let's get 1M rows from TPCH lineitem table in EU cluster"
    desc "\nWithout spooling:"
    run_command "no_encoding_eu" ${TRINO_COMMAND_EU[@]} \
    --network-logging=BASIC --output-format=null \
    --encoding="" \
    --execute '''SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'''
    desc "\nWith spooled '${GREEN}json${CC}' encoding:"
    run_command "json_eu" ${TRINO_COMMAND_EU[@]} \
        --network-logging=BASIC --output-format=null \
        --encoding="json" \
        --execute 'SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'
    desc "\nWith spooled '${GREEN}json+zstd${CC}' encoding:"
    run_command "json_zstd_eu" ${TRINO_COMMAND_EU[@]} \
        --output-format=null \
        --encoding="json+zstd" \
        --execute 'SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'
    desc "\nWith spooled '${GREEN}json+lz4${CC}' encoding:"
    run_command "json_lz4_eu" ${TRINO_COMMAND_EU[@]} \
        --output-format=null \
        --encoding="json+lz4" \
        --execute 'SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'
    nextOrPrevious 1
}

function example2
{
    header "Let's get 1M rows from TPCH lineitem table in US cluster"
    desc "\nWithout spooling:"
    run_command "no_encoding_us" ${TRINO_COMMAND_US[@]} \
        --output-format=null \
        --encoding="" \
        --execute 'SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'
    desc "\nWith spooled '${GREEN}json${CC}' encoding:"
    run_command "json_us" ${TRINO_COMMAND_US[@]} \
        --output-format=null \
        --encoding="json" \
        --execute 'SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'
    desc "\nWith spooled '${GREEN}json+zstd${CC}' encoding:"
    run_command "json_zstd_us" ${TRINO_COMMAND_US[@]} \
        --output-format=null \
        --encoding="json+zstd" \
        --execute 'SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'
    desc "\nWith spooled '${GREEN}json+lz4${CC}' encoding:"
    run_command "json_lz4_us" ${TRINO_COMMAND_US[@]} \
        --output-format=null \
        --encoding="json+lz4" \
        --execute 'SELECT * FROM tpch.sf10.lineitem LIMIT 1_000_000'
    nextOrPrevious 2
}

function example3
{
    desc "Let's summarize measured times:"

    summary

    echo  -ne "\n\n\n${BLUE}That's all! Thank you${CC} 🎉\n\n\n"
    exit 0
}

example0
example1
