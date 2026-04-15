#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-newslab}"
INTERVAL="${INTERVAL:-5}"
OUT_CSV="${OUT_CSV:-dataset_with_scaling_$(date -u +%Y%m%dT%H%M%SZ).csv}"

MASTER_LABEL="${MASTER_LABEL:-app=redis,role=master}"
REPLICA_LABEL="${REPLICA_LABEL:-app=redis,role=replica}"
REPLICA_DEPLOY="${REPLICA_DEPLOY:-redis-replicas}"

MASTER_HOST="${MASTER_HOST:-redis-master}"
MASTER_PORT="${MASTER_PORT:-6379}"
REPLICA_HOST="${REPLICA_HOST:-redis-replicas}"
REPLICA_PORT="${REPLICA_PORT:-6379}"

COOLDOWN="${COOLDOWN:-60}"
MIN="${MIN:-1}"
MAX="${MAX:-10}"
CPU_UP_PCT="${CPU_UP_PCT:-80}"
CPU_DOWN_PCT="${CPU_DOWN_PCT:-50}"
DOWNSCALE_STREAK_N="${DOWNSCALE_STREAK_N:-5}"
CPU_LIMIT_CORES="${CPU_LIMIT_CORES:-0.5}"
SCALE_MODE="${SCALE_MODE:-max}"
ENABLE_SCALING="${ENABLE_SCALING:-true}"

LATENCY_SAMPLES="${LATENCY_SAMPLES:-7}"
LATENCY_TIMEOUT_SEC="${LATENCY_TIMEOUT_SEC:-2}"
LATENCY_KEY="${LATENCY_KEY:-autoscaler:latency_probe}"
LATENCY_VALUE="${LATENCY_VALUE:-1}"

# Fallback memory reference used only when Redis maxmemory=0.
# Default: 1 GiB
REDIS_MEM_REF_BYTES="${REDIS_MEM_REF_BYTES:-1073741824}"

declare -A PREV_CPU=()
declare -A PREV_T=()

last_scale=0
down_streak=0

ts_utc() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
epoch_s() { date +%s; }

float_gt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a > b) }'; }
float_lt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a < b) }'; }
float_add() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.6f", a + b }'; }
float_div() { awk -v a="$1" -v b="$2" 'BEGIN { if (b == 0) print "0.00"; else printf "%.2f", a / b }'; }

csv_escape() {
  local s="${1:-}"
  s="${s//\"/\"\"}"
  printf '"%s"' "$s"
}

get_deploy_field() {
  local deploy="$1"
  local jsonpath="$2"
  kubectl -n "$NS" get deploy "$deploy" -o jsonpath="$jsonpath" 2>/dev/null || true
}

get_replicas() {
  local n
  n="$(get_deploy_field "$REPLICA_DEPLOY" '{.spec.replicas}')"
  [ -n "$n" ] && echo "$n" || echo 1
}

set_replicas() {
  local n="$1"
  kubectl -n "$NS" patch deployment "$REPLICA_DEPLOY" -p "{\"spec\":{\"replicas\":$n}}" >/dev/null 2>&1
}

get_pods_by_label() {
  local label="$1"
  kubectl -n "$NS" get pods -l "$label" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
}

get_running_pods_by_label() {
  local label="$1"
  kubectl -n "$NS" get pods -l "$label" \
    -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
}

get_first_pod() {
  local label="$1"
  get_pods_by_label "$label" | head -n 1
}

pod_container_ready() {
  local pod="${1:-}"
  [ -z "$pod" ] && { echo 0; return; }
  local out
  out="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null | tr '[:upper:]' '[:lower:]' || true)"
  [ "$out" = "true" ] && echo 1 || echo 0
}

list_ready_replica_pods() {
  local pods p
  pods="$(get_running_pods_by_label "$REPLICA_LABEL")"
  [ -z "$pods" ] && return 0
  while IFS= read -r p; do
    [ -z "$p" ] && continue
    if [ "$(pod_container_ready "$p")" = "1" ]; then
      echo "$p"
    fi
  done <<< "$pods"
}

redis_info_section() {
  local pod="$1"
  local section="$2"
  [ -z "$pod" ] && return 0
  kubectl -n "$NS" exec "$pod" -- sh -c "redis-cli INFO $section 2>/dev/null" 2>/dev/null || true
}

redis_info_get() {
  local pod="$1"
  local section="$2"
  local key="$3"
  redis_info_section "$pod" "$section" | awk -F: -v k="$key" '
    $1 == k {
      gsub("\r", "", $2)
      print $2
      exit
    }'
}

read_redis_cpu_total_s() {
  local pod="$1"
  kubectl -n "$NS" exec "$pod" -- sh -c '
    redis-cli INFO cpu 2>/dev/null | awk -F: "
      /^used_cpu_sys:/  {sys=\$2}
      /^used_cpu_user:/ {usr=\$2}
      END {
        gsub(\"\\r\",\"\",sys)
        gsub(\"\\r\",\"\",usr)
        if (sys==\"\" || usr==\"\") { print \"\"; exit }
        printf(\"%.6f\", sys + usr)
      }"
  ' 2>/dev/null | tail -n 1 | tr -d '\r'
}

compute_mem_pct_and_ref() {
  local used="$1"
  local maxm="$2"
  local ref="$REDIS_MEM_REF_BYTES"

  if [ -n "$maxm" ] && [ "$maxm" -gt 0 ] 2>/dev/null; then
    ref="$maxm"
  fi

  awk -v used="$used" -v ref="$ref" '
    BEGIN {
      if (ref <= 0) {
        printf "0.00,%s", ref
        exit
      }
      printf "%.2f,%s", (used / ref) * 100.0, ref
    }'
}

latency_probe_redis_cli() {
  local host="$1"
  local port="$2"
  local mode="$3"

  local vals=()
  local i ms
  local tmp="/tmp/lat_vals.$$.$mode.$RANDOM"

  i=1
  while [ "$i" -le "$LATENCY_SAMPLES" ]; do
    case "$mode" in
      get)
        local t0 t1
        t0="$(date +%s%N)"
        timeout "${LATENCY_TIMEOUT_SEC}s" redis-cli -h "$host" -p "$port" GET "$LATENCY_KEY" >/dev/null 2>&1 || true
        t1="$(date +%s%N)"
        ms="$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.3f", (b-a)/1000000.0 }')"
        vals+=("$ms")
        ;;
      set)
        local t0 t1
        t0="$(date +%s%N)"
        timeout "${LATENCY_TIMEOUT_SEC}s" redis-cli -h "$host" -p "$port" SET "$LATENCY_KEY" "$LATENCY_VALUE" >/dev/null 2>&1 || true
        t1="$(date +%s%N)"
        ms="$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.3f", (b-a)/1000000.0 }')"
        vals+=("$ms")
        ;;
    esac
    i=$((i+1))
  done

  if [ "${#vals[@]}" -eq 0 ]; then
    echo "0,0,0,0,0"
    return
  fi

  printf "%s\n" "${vals[@]}" | sort -n > "$tmp"

  local count sum mean p50 p95 max
  count="${#vals[@]}"
  sum="$(awk '{s+=$1} END { printf "%.3f", s+0 }' "$tmp")"
  mean="$(awk -v s="$sum" -v c="$count" 'BEGIN { if (c==0) print "0.000"; else printf "%.3f", s/c }')"
  p50="$(awk -v c="$count" '{ a[NR]=$1 } END { idx=int((c-1)*0.50)+1; printf "%.3f", a[idx] }' "$tmp")"
  p95="$(awk -v c="$count" '{ a[NR]=$1 } END { idx=int((c-1)*0.95)+1; if (idx<1) idx=1; if (idx>c) idx=c; printf "%.3f", a[idx] }' "$tmp")"
  max="$(tail -n 1 "$tmp")"

  rm -f "$tmp"
  echo "$count,$mean,$p50,$p95,$max"
}

write_header() {
  cat > "$OUT_CSV" <<'EOF'
ts,epoch_s,leader_pod,leader_ready,replica_pod_sample,replica_count_spec,replica_count_ready,replica_pods_observed,replica_pods_measured,replica_pods_baselined,leader_ops,leader_in_kbps,leader_out_kbps,leader_used_mem_bytes,leader_mem_ref_bytes,leader_mem_pct,leader_cpu_sys,leader_cpu_user,leader_connected_clients,leader_connected_slaves,replica_ops_sample,replica_ops_sum_est,replica_in_kbps_sample,replica_out_kbps_sample,replica_used_mem_bytes_sample,replica_mem_ref_bytes_sample,replica_mem_pct_sample,replica_cpu_sys_sample,replica_cpu_user_sample,replica_connected_clients_sample,replica_master_link_status_sample,replica_master_last_io_s_sample,lat_set_master_count,lat_set_master_mean_ms,lat_set_master_p50_ms,lat_set_master_p95_ms,lat_set_master_max_ms,lat_get_replica_count,lat_get_replica_mean_ms,lat_get_replica_p50_ms,lat_get_replica_p95_ms,lat_get_replica_max_ms,cpu_avg_pct,cpu_max_pct,metric_pct,cpu_limit_cores,scale_mode,current_replicas,desired_replicas,can_scale,action,down_streak,last_scale_epoch_s,scaling_enabled
EOF
}

echo "Starting collector+scaler: ns=$NS dep=$REPLICA_DEPLOY interval=$INTERVAL cooldown=$COOLDOWN mode=$SCALE_MODE cpu_limit_cores=$CPU_LIMIT_CORES mem_ref_bytes=$REDIS_MEM_REF_BYTES"
echo "writing_csv=$OUT_CSV"

write_header

while true; do
  ts="$(ts_utc)"
  now_s="$(epoch_s)"

  leader_pod="$(get_first_pod "$MASTER_LABEL")"
  leader_ready="$(pod_container_ready "$leader_pod")"

  mapfile -t replica_pods < <(get_pods_by_label "$REPLICA_LABEL" || true)
  replica_sample="${replica_pods[0]:-}"

  replica_spec="$(get_deploy_field "$REPLICA_DEPLOY" '{.spec.replicas}')"
  replica_ready="$(get_deploy_field "$REPLICA_DEPLOY" '{.status.readyReplicas}')"
  [ -z "$replica_spec" ] && replica_spec=0
  [ -z "$replica_ready" ] && replica_ready=0

  leader_ops="$(redis_info_get "$leader_pod" stats instantaneous_ops_per_sec)"; [ -z "$leader_ops" ] && leader_ops=0
  leader_in_kbps="$(redis_info_get "$leader_pod" stats instantaneous_input_kbps)"; [ -z "$leader_in_kbps" ] && leader_in_kbps=0
  leader_out_kbps="$(redis_info_get "$leader_pod" stats instantaneous_output_kbps)"; [ -z "$leader_out_kbps" ] && leader_out_kbps=0
  leader_used_mem_bytes="$(redis_info_get "$leader_pod" memory used_memory)"; [ -z "$leader_used_mem_bytes" ] && leader_used_mem_bytes=0
  leader_maxmemory_bytes="$(redis_info_get "$leader_pod" memory maxmemory)"; [ -z "$leader_maxmemory_bytes" ] && leader_maxmemory_bytes=0
  IFS=, read -r leader_mem_pct leader_mem_ref_bytes <<< "$(compute_mem_pct_and_ref "$leader_used_mem_bytes" "$leader_maxmemory_bytes")"
  leader_cpu_sys="$(redis_info_get "$leader_pod" cpu used_cpu_sys)"; [ -z "$leader_cpu_sys" ] && leader_cpu_sys=0
  leader_cpu_user="$(redis_info_get "$leader_pod" cpu used_cpu_user)"; [ -z "$leader_cpu_user" ] && leader_cpu_user=0
  leader_connected_clients="$(redis_info_get "$leader_pod" clients connected_clients)"; [ -z "$leader_connected_clients" ] && leader_connected_clients=0
  leader_connected_slaves="$(redis_info_get "$leader_pod" replication connected_slaves)"; [ -z "$leader_connected_slaves" ] && leader_connected_slaves=0

  replica_ops_sample="$(redis_info_get "$replica_sample" stats instantaneous_ops_per_sec)"; [ -z "$replica_ops_sample" ] && replica_ops_sample=0
  replica_in_kbps_sample="$(redis_info_get "$replica_sample" stats instantaneous_input_kbps)"; [ -z "$replica_in_kbps_sample" ] && replica_in_kbps_sample=0
  replica_out_kbps_sample="$(redis_info_get "$replica_sample" stats instantaneous_output_kbps)"; [ -z "$replica_out_kbps_sample" ] && replica_out_kbps_sample=0
  replica_used_mem_bytes_sample="$(redis_info_get "$replica_sample" memory used_memory)"; [ -z "$replica_used_mem_bytes_sample" ] && replica_used_mem_bytes_sample=0
  replica_maxmemory_bytes_sample="$(redis_info_get "$replica_sample" memory maxmemory)"; [ -z "$replica_maxmemory_bytes_sample" ] && replica_maxmemory_bytes_sample=0
  IFS=, read -r replica_mem_pct_sample replica_mem_ref_bytes_sample <<< "$(compute_mem_pct_and_ref "$replica_used_mem_bytes_sample" "$replica_maxmemory_bytes_sample")"
  replica_cpu_sys_sample="$(redis_info_get "$replica_sample" cpu used_cpu_sys)"; [ -z "$replica_cpu_sys_sample" ] && replica_cpu_sys_sample=0
  replica_cpu_user_sample="$(redis_info_get "$replica_sample" cpu used_cpu_user)"; [ -z "$replica_cpu_user_sample" ] && replica_cpu_user_sample=0
  replica_connected_clients_sample="$(redis_info_get "$replica_sample" clients connected_clients)"; [ -z "$replica_connected_clients_sample" ] && replica_connected_clients_sample=0
  replica_master_link_status_sample="$(redis_info_get "$replica_sample" replication master_link_status)"
  replica_master_last_io_s_sample="$(redis_info_get "$replica_sample" replication master_last_io_seconds_ago)"; [ -z "$replica_master_last_io_s_sample" ] && replica_master_last_io_s_sample=-1

  replica_ops_sum_est=0
  for p in "${replica_pods[@]:-}"; do
    [ -z "$p" ] && continue
    v="$(redis_info_get "$p" stats instantaneous_ops_per_sec)"
    [ -z "$v" ] && v=0
    replica_ops_sum_est=$((replica_ops_sum_est + v))
  done

  IFS=, read -r lat_set_master_count lat_set_master_mean_ms lat_set_master_p50_ms lat_set_master_p95_ms lat_set_master_max_ms \
    <<< "$(latency_probe_redis_cli "$MASTER_HOST" "$MASTER_PORT" set)"
  IFS=, read -r lat_get_replica_count lat_get_replica_mean_ms lat_get_replica_p50_ms lat_get_replica_p95_ms lat_get_replica_max_ms \
    <<< "$(latency_probe_redis_cli "$REPLICA_HOST" "$REPLICA_PORT" get)"

  mapfile -t ready_replica_pods < <(list_ready_replica_pods || true)

  declare -A LIVE_PODS=()
  for pod in "${ready_replica_pods[@]:-}"; do
    [ -n "$pod" ] && LIVE_PODS["$pod"]=1
  done
  for pod in "${!PREV_CPU[@]}"; do
    if [ -z "${LIVE_PODS[$pod]+x}" ]; then
      unset 'PREV_CPU[$pod]'
      unset 'PREV_T[$pod]'
    fi
  done

  util_sum="0.0"
  util_max="0.0"
  util_cnt=0
  baselined_cnt=0

  for pod in "${ready_replica_pods[@]:-}"; do
    [ -z "$pod" ] && continue
    cpu_total_s="$(read_redis_cpu_total_s "$pod" || true)"
    [ -z "${cpu_total_s:-}" ] && continue

    if [ -z "${PREV_CPU[$pod]+x}" ] || [ -z "${PREV_T[$pod]+x}" ]; then
      PREV_CPU["$pod"]="$cpu_total_s"
      PREV_T["$pod"]="$now_s"
      baselined_cnt=$((baselined_cnt + 1))
      continue
    fi

    prev_cpu="${PREV_CPU[$pod]}"
    prev_t="${PREV_T[$pod]}"
    dt_s=$(( now_s - prev_t ))

    PREV_CPU["$pod"]="$cpu_total_s"
    PREV_T["$pod"]="$now_s"

    if [ "$dt_s" -le 0 ]; then
      baselined_cnt=$((baselined_cnt + 1))
      continue
    fi

    util_pct="$(awk -v prev="$prev_cpu" -v curv="$cpu_total_s" -v dt="$dt_s" -v limit="$CPU_LIMIT_CORES" '
      BEGIN {
        du = curv - prev
        if (du < 0) du = 0
        cores_used = (dt > 0) ? (du / dt) : 0
        util = (limit > 0) ? ((cores_used / limit) * 100.0) : 0
        if (util < 0) util = 0
        printf "%.2f", util
      }'
    )"

    util_sum="$(float_add "$util_sum" "$util_pct")"
    util_max="$(awk -v a="$util_max" -v b="$util_pct" 'BEGIN { if (b > a) printf "%.2f", b; else printf "%.2f", a }')"
    util_cnt=$((util_cnt + 1))
  done

  if [ "$util_cnt" -eq 0 ]; then
    cpu_avg_pct="0.00"
    cpu_max_pct="0.00"
    metric_pct="0.00"
  else
    cpu_avg_pct="$(float_div "$util_sum" "$util_cnt")"
    cpu_max_pct="$util_max"
    if [ "$SCALE_MODE" = "max" ]; then
      metric_pct="$cpu_max_pct"
    else
      metric_pct="$cpu_avg_pct"
    fi
  fi

  current_replicas="$replica_spec"
  [ "$current_replicas" -le 0 ] && current_replicas="$(get_replicas)"
  desired_replicas="$current_replicas"
  action="none"

  can_scale=0
  if (( now_s - last_scale >= COOLDOWN )); then
    can_scale=1
  fi

  if [ "${#ready_replica_pods[@]}" -eq 0 ]; then
    down_streak=0
    action="no_ready_replica_pods"
  elif [ "$util_cnt" -eq 0 ]; then
    down_streak=0
    action="baseline_only"
  else
    if float_gt "$metric_pct" "$CPU_UP_PCT"; then
      down_streak=0
      if (( can_scale == 1 )) && (( current_replicas < MAX )); then
        desired_replicas=$(( current_replicas + 1 ))
        (( desired_replicas > MAX )) && desired_replicas="$MAX"
        if [ "$ENABLE_SCALING" = "true" ]; then
          if set_replicas "$desired_replicas"; then
            last_scale="$now_s"
            action="up ${current_replicas}->${desired_replicas}"
          else
            desired_replicas="$current_replicas"
            action="up_patch_failed"
          fi
        else
          action="up_planned ${current_replicas}->${desired_replicas}"
        fi
      else
        action="up_candidate"
      fi
    elif float_lt "$metric_pct" "$CPU_DOWN_PCT"; then
      if (( current_replicas > MIN )); then
        down_streak=$((down_streak + 1))
        if (( can_scale == 1 )) && (( down_streak >= DOWNSCALE_STREAK_N )); then
          desired_replicas=$(( current_replicas - 1 ))
          (( desired_replicas < MIN )) && desired_replicas="$MIN"
          if [ "$ENABLE_SCALING" = "true" ]; then
            if set_replicas "$desired_replicas"; then
              last_scale="$now_s"
              down_streak=0
              action="down ${current_replicas}->${desired_replicas}"
            else
              desired_replicas="$current_replicas"
              action="down_patch_failed"
            fi
          else
            action="down_planned ${current_replicas}->${desired_replicas}"
          fi
        else
          action="down_candidate streak=${down_streak}/${DOWNSCALE_STREAK_N}"
        fi
      else
        down_streak=0
        action="at_min"
      fi
    else
      down_streak=0
      action="within_band"
    fi
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
    "$(csv_escape "$ts")" "$(csv_escape "$now_s")" "$(csv_escape "$leader_pod")" "$(csv_escape "$leader_ready")" \
    "$(csv_escape "$replica_sample")" "$(csv_escape "$replica_spec")" "$(csv_escape "$replica_ready")" \
    "$(csv_escape "${#replica_pods[@]}")" "$(csv_escape "$util_cnt")" "$(csv_escape "$baselined_cnt")" >> "$OUT_CSV"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
    "$(csv_escape "$leader_ops")" "$(csv_escape "$leader_in_kbps")" "$(csv_escape "$leader_out_kbps")" \
    "$(csv_escape "$leader_used_mem_bytes")" "$(csv_escape "$leader_mem_ref_bytes")" "$(csv_escape "$leader_mem_pct")" \
    "$(csv_escape "$leader_cpu_sys")" "$(csv_escape "$leader_cpu_user")" \
    "$(csv_escape "$leader_connected_clients")" "$(csv_escape "$leader_connected_slaves")" >> "$OUT_CSV"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
    "$(csv_escape "$replica_ops_sample")" "$(csv_escape "$replica_ops_sum_est")" "$(csv_escape "$replica_in_kbps_sample")" \
    "$(csv_escape "$replica_out_kbps_sample")" "$(csv_escape "$replica_used_mem_bytes_sample")" \
    "$(csv_escape "$replica_mem_ref_bytes_sample")" "$(csv_escape "$replica_mem_pct_sample")" \
    "$(csv_escape "$replica_cpu_sys_sample")" "$(csv_escape "$replica_cpu_user_sample")" \
    "$(csv_escape "$replica_connected_clients_sample")" "$(csv_escape "$replica_master_link_status_sample")" \
    "$(csv_escape "$replica_master_last_io_s_sample")" >> "$OUT_CSV"

  printf '%s,%s,%s,%s,%s,' \
    "$(csv_escape "$lat_set_master_count")" "$(csv_escape "$lat_set_master_mean_ms")" "$(csv_escape "$lat_set_master_p50_ms")" \
    "$(csv_escape "$lat_set_master_p95_ms")" "$(csv_escape "$lat_set_master_max_ms")" >> "$OUT_CSV"

  printf '%s,%s,%s,%s,%s,' \
    "$(csv_escape "$lat_get_replica_count")" "$(csv_escape "$lat_get_replica_mean_ms")" "$(csv_escape "$lat_get_replica_p50_ms")" \
    "$(csv_escape "$lat_get_replica_p95_ms")" "$(csv_escape "$lat_get_replica_max_ms")" >> "$OUT_CSV"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(csv_escape "$cpu_avg_pct")" "$(csv_escape "$cpu_max_pct")" "$(csv_escape "$metric_pct")" \
    "$(csv_escape "$CPU_LIMIT_CORES")" "$(csv_escape "$SCALE_MODE")" "$(csv_escape "$current_replicas")" \
    "$(csv_escape "$desired_replicas")" "$(csv_escape "$can_scale")" "$(csv_escape "$action")" \
    "$(csv_escape "$down_streak")" "$(csv_escape "$last_scale")" \
    "$(csv_escape "$([ "$ENABLE_SCALING" = "true" ] && echo 1 || echo 0)")" >> "$OUT_CSV"

  echo "{\"ts\":\"$ts\",\"action\":\"$action\",\"current\":$current_replicas,\"desired\":$desired_replicas,\"cpu_avg_pct\":$cpu_avg_pct,\"cpu_max_pct\":$cpu_max_pct,\"metric_pct\":$metric_pct,\"leader_mem_pct\":$leader_mem_pct,\"replica_mem_pct_sample\":$replica_mem_pct_sample}"

  sleep "$INTERVAL"
done