#!/usr/bin/env bash
set -euo pipefail

# ---- Constants ----
readonly ADVISORY_LOCK_KEY="${ADVISORY_LOCK_KEY:-28110994}"
readonly DB_CHECK_TIMEOUT="${DB_CHECK_TIMEOUT:-300}"  # 5 minutes
readonly POSTGRES_DEFAULT_DB="postgres"

# ---- Configuration ----
setup_environment() {
  echo "[entrypoint] rendering configs ..."
  envsubst < "$HIVE_HOME/conf/core-site.xml.tmpl" > "$HIVE_HOME/conf/core-site.xml"
  envsubst < "$HIVE_HOME/conf/hive-site.xml.tmpl" > "$HIVE_HOME/conf/hive-site.xml"

  HOST="${HIVE_POSTGRES_HOST:?HIVE_POSTGRES_HOST is required}"
  PORT="${HIVE_POSTGRES_PORT:-5432}"
  DB="${HIVE_POSTGRES_DB:-metastore}"
  USER="${HIVE_DB_USER:?HIVE_DB_USER is required}"
  PASS="${HIVE_DB_PASSWORD:?HIVE_DB_PASSWORD is required}"
  CREATE_DB_IF_MISSING="${CREATE_DB_IF_MISSING:-true}"
  JDBC_EXTRA_PARAMS="${JDBC_EXTRA_PARAMS:-sslmode=require}"

  JDBC_URL="jdbc:postgresql://${HOST}:${PORT}/${DB}?${JDBC_EXTRA_PARAMS}"
  export PGPASSWORD="${PASS}"

  readonly HOST PORT DB USER PASS CREATE_DB_IF_MISSING JDBC_URL
}

# ---- Database Setup Functions ----
wait_for_postgres() {
  echo "[entrypoint] waiting for postgres ${HOST}:${PORT} ..."
  local timeout=${DB_CHECK_TIMEOUT}
  while ! pg_isready -h "${HOST}" -p "${PORT}" -d "${POSTGRES_DEFAULT_DB}" >/dev/null 2>&1; do
    sleep 1
    timeout=$((timeout - 1))
    if [[ $timeout -le 0 ]]; then
      echo "[entrypoint] ERROR: timeout waiting for postgres after ${DB_CHECK_TIMEOUT}s"
      exit 1
    fi
  done
  echo "[entrypoint] postgres is accepting connections."
}

create_database_if_missing() {
  if [[ "${CREATE_DB_IF_MISSING}" != "true" ]]; then
    echo "[entrypoint] CREATE_DB_IF_MISSING=false; skipping DB create."
    return 0
  fi

  if psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -Atqc "select 1" >/dev/null 2>&1; then
    echo "[entrypoint] database \"${DB}\" exists."
    return 0
  fi

  echo "[entrypoint] database \"${DB}\" not found; attempting to create ..."
  if ! psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${POSTGRES_DEFAULT_DB}" -v ON_ERROR_STOP=1 \
       -c "CREATE DATABASE ${DB} TEMPLATE template0 ENCODING 'UTF8';"; then
    echo "[entrypoint] WARNING: database create failed (insufficient privileges?)"
    echo "[entrypoint] If using a limited user, pre-create DB ${DB} once and set CREATE_DB_IF_MISSING=false."
    return 1
  fi
  echo "[entrypoint] database \"${DB}\" created successfully."
}

acquire_advisory_lock() {
  echo "[entrypoint] acquiring advisory lock (${ADVISORY_LOCK_KEY}) ..."
  psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -v ON_ERROR_STOP=1 \
    -c "SELECT pg_advisory_lock(${ADVISORY_LOCK_KEY});"
}

release_advisory_lock() {
  echo "[entrypoint] releasing advisory lock ..."
  psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -v ON_ERROR_STOP=1 \
    -c "SELECT pg_advisory_unlock(${ADVISORY_LOCK_KEY});" || true
}

cleanup() {
  release_advisory_lock
}

# ---- Schema Management Functions ----
run_schematool() {
  local operation="$1"
  local description="$2"
  
  echo "[entrypoint] ${description} ..."
  set +e
  /opt/hive/bin/schematool \
    -dbType postgres \
    -"${operation}" \
    -userName "${USER}" \
    -passWord "${PASS}" \
    -url "${JDBC_URL}" \
    --verbose
  local rc=$?
  set -e
  echo "[entrypoint] schematool -${operation} returned code: ${rc}"
  return ${rc}
}

initialize_schema() {

  echo "[entrypoint] ===== SCHEMA MANAGEMENT PHASE ====="
  echo "[entrypoint] validating schema ..."
  if /opt/hive/bin/schematool \
    -dbType postgres \
    -validate \
    -userName "${USER}" \
    -passWord "${PASS}" \
    -url "${JDBC_URL}" \
    --verbose; then
    echo "[entrypoint] ✅ schema validation successful - metastore ready!"
    echo "[entrypoint] ===== SCHEMA MANAGEMENT COMPLETE ====="
    return 0
  else
    echo "[entrypoint] ℹ️  schema validation failed; attempting initialization and upgrade ..."
    if run_schematool "initOrUpgradeSchema" "schema init or upgrade"; then
      echo "[entrypoint] ✅ schema initialized or upgraded successfully."
    else
      echo "[entrypoint] ⚠️  initOrUpgradeSchema returned non-zero; check logs above (may be harmless if already current)."
    fi
    echo "[entrypoint] performing final schema validation ..."
    if /opt/hive/bin/schematool \
      -dbType postgres \
      -validate \
      -userName "${USER}" \
      -passWord "${PASS}" \
      -url "${JDBC_URL}" \
      --verbose; then
      echo "[entrypoint] ✅ schema validation successful - metastore ready!"
      echo "[entrypoint] ===== SCHEMA MANAGEMENT COMPLETE ====="
      return 0
    else
      echo "[entrypoint] ❌ schema validation failed - metastore may not work correctly"
      exit 1
    fi
  fi
}

# ---- Main Execution ----
main() {
  setup_environment
  wait_for_postgres
  create_database_if_missing
  
  acquire_advisory_lock
  trap cleanup EXIT
  
  initialize_schema
  
  echo "[entrypoint] starting Hive Metastore ..."
  exec /opt/hive/bin/start-metastore
}

# Run main function
main "$@"
