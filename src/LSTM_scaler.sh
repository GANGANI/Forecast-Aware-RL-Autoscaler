#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-newslab}"
INTERVAL="${INTERVAL:-5}"
OUT_CSV="${OUT_CSV:-dataset_predictive_only_$(date -u +%Y%m%dT%H%M%SZ).csv}"

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
CPU_LIMIT_CORES="${CPU_LIMIT_CORES:-0.5}"
ENABLE_SCALING="${ENABLE_SCALING:-true}"

LATENCY_SAMPLES="${LATENCY_SAMPLES:-7}"
LATENCY_TIMEOUT_SEC="${LATENCY_TIMEOUT_SEC:-2}"
LATENCY_KEY="${LATENCY_KEY:-autoscaler:latency_probe}"
LATENCY_VALUE="${LATENCY_VALUE:-1}"

REDIS_MEM_REF_BYTES="${REDIS_MEM_REF_BYTES:-1073741824}"

ENABLE_PREDICTIVE_SCALING="${ENABLE_PREDICTIVE_SCALING:-true}"
MODEL_PATH="${MODEL_PATH:-lstm_forecaster.pt}"
SCALER_PATH="${SCALER_PATH:-lstm_scalers.joblib}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SEQ_WINDOW="${SEQ_WINDOW:-8}"
SEQ_FILE="${SEQ_FILE:-/tmp/lstm_sequence_buffer.csv}"

PREDICT_CPU_UP_PCT="${PREDICT_CPU_UP_PCT:-80}"
PREDICT_CPU_DOWN_PCT="${PREDICT_CPU_DOWN_PCT:-60}"

declare -A PREV_CPU=()
declare -A PREV_T=()

last_scale=0
sample_count=0

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
      *)
        echo "0,0,0,0,0"
        return
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

init_seq_file() {
  if [ ! -f "$SEQ_FILE" ]; then
    cat > "$SEQ_FILE" <<'EOF'
current_replicas,cpu_usage,memory_usage,traffic_in,traffic_out
EOF
  fi
}

append_seq_row() {
  local current_replicas="$1"
  local cpu_usage="$2"
  local memory_usage="$3"
  local traffic_in="$4"
  local traffic_out="$5"

  printf '%s,%s,%s,%s,%s\n' \
    "$current_replicas" "$cpu_usage" "$memory_usage" \
    "$traffic_in" "$traffic_out" >> "$SEQ_FILE"

  local total_lines keep_lines
  total_lines="$(wc -l < "$SEQ_FILE")"
  keep_lines=$((SEQ_WINDOW + 1))

  if [ "$total_lines" -gt "$keep_lines" ]; then
    {
      head -n 1 "$SEQ_FILE"
      tail -n "$SEQ_WINDOW" "$SEQ_FILE"
    } > "${SEQ_FILE}.tmp"
    mv "${SEQ_FILE}.tmp" "$SEQ_FILE"
  fi
}

predict_next_state_from_file() {
  "$PYTHON_BIN" - "$SEQ_FILE" "$MODEL_PATH" "$SCALER_PATH" "$SEQ_WINDOW" <<'PY'
import sys
import json
import pandas as pd

seq_file = sys.argv[1]
model_path = sys.argv[2]
scaler_path = sys.argv[3]
seq_window = int(sys.argv[4])

FEATURE_COLS = [
    "current_replicas",
    "cpu_usage",
    "memory_usage",
    "traffic_in",
    "traffic_out",
]

TARGET_COLS = [
    "cpu_usage",
    "memory_usage",
]

TARGET_CPU_IDX = 0
TARGET_MEM_IDX = 1

try:
    import joblib
    import torch
    import torch.nn as nn

    class LSTMForecaster(nn.Module):
        def __init__(self, input_size, hidden_size, num_layers, output_size, dropout=0.0):
            super().__init__()
            self.lstm = nn.LSTM(
                input_size=input_size,
                hidden_size=hidden_size,
                num_layers=num_layers,
                batch_first=True,
                dropout=dropout if num_layers > 1 else 0.0,
            )
            self.fc = nn.Linear(hidden_size, output_size)

        def forward(self, x):
            out, _ = self.lstm(x)
            out = out[:, -1, :]
            return self.fc(out)

    df = pd.read_csv(seq_file)

    if len(df) < seq_window:
        print(json.dumps({
            "ok": False,
            "pred_cpu_t1": "",
            "pred_mem_t1": "",
            "reason": f"insufficient_sequence:{len(df)}/{seq_window}"
        }))
        sys.exit(0)

    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        print(json.dumps({
            "ok": False,
            "pred_cpu_t1": "",
            "pred_mem_t1": "",
            "reason": "missing_columns:" + ",".join(missing)
        }))
        sys.exit(0)

    sequence = df[FEATURE_COLS].tail(seq_window).astype(float).values

    scalers = joblib.load(scaler_path)
    if not isinstance(scalers, dict):
        raise TypeError(f"expected scaler dict, got {type(scalers).__name__}")

    if "x_scaler" not in scalers or "y_scaler" not in scalers:
        raise KeyError("scaler dict must contain x_scaler and y_scaler")

    x_scaler = scalers["x_scaler"]
    y_scaler = scalers["y_scaler"]

    seq_scaled = x_scaler.transform(sequence)
    x = torch.tensor(seq_scaled, dtype=torch.float32).unsqueeze(0)

    state = torch.load(model_path, map_location="cpu")
    if not isinstance(state, dict):
        raise TypeError(f"expected state_dict, got {type(state).__name__}")

    if "lstm.weight_ih_l0" not in state or "fc.weight" not in state:
      raise KeyError("state_dict missing required keys: lstm.weight_ih_l0 / fc.weight")

    input_size = state["lstm.weight_ih_l0"].shape[1]
    hidden_size = state["lstm.weight_ih_l0"].shape[0] // 4
    output_size = state["fc.weight"].shape[0]

    num_layers = 0
    while f"lstm.weight_ih_l{num_layers}" in state:
        num_layers += 1

    if input_size != len(FEATURE_COLS):
        raise ValueError(f"model input_size={input_size}, expected {len(FEATURE_COLS)}")

    if output_size != len(TARGET_COLS):
        raise ValueError(f"model output_size={output_size}, expected {len(TARGET_COLS)}")

    if hasattr(y_scaler, "n_features_in_") and int(y_scaler.n_features_in_) != output_size:
        raise ValueError(
            f"y_scaler expects {int(y_scaler.n_features_in_)} outputs but model outputs {output_size}"
        )

    model = LSTMForecaster(
        input_size=input_size,
        hidden_size=hidden_size,
        num_layers=num_layers,
        output_size=output_size,
        dropout=0.0,
    )
    model.load_state_dict(state)
    model.eval()

    with torch.no_grad():
        pred_scaled = model(x).cpu().numpy()

    pred = y_scaler.inverse_transform(pred_scaled)

    if pred.shape[1] < 2:
        raise ValueError(f"expected at least 2 targets, got shape {pred.shape}")

    pred_cpu_t1 = float(pred[0][TARGET_CPU_IDX])
    pred_mem_t1 = float(pred[0][TARGET_MEM_IDX])

    print(json.dumps({
        "ok": True,
        "pred_cpu_t1": pred_cpu_t1,
        "pred_mem_t1": pred_mem_t1,
        "reason": ""
    }))

except Exception as e:
    print(json.dumps({
        "ok": False,
        "pred_cpu_t1": "",
        "pred_mem_t1": "",
        "reason": str(e)
    }))
PY
}

select_action_predictive() {
  local pred_cpu_t1="$1"
  local current_replicas="$2"
  local can_scale="$3"

  local desired="$current_replicas"
  local action="predict_within_band"

  if float_gt "$pred_cpu_t1" "$PREDICT_CPU_UP_PCT"; then
    if (( can_scale == 1 )) && (( current_replicas < MAX )); then
      desired=$((current_replicas + 1))
      (( desired > MAX )) && desired="$MAX"
      action="predictive_up ${current_replicas}->${desired}"
    else
      action="predictive_up_candidate"
    fi
  elif float_lt "$pred_cpu_t1" "$PREDICT_CPU_DOWN_PCT"; then
    if (( can_scale == 1 )) && (( current_replicas > MIN )); then
      desired=$((current_replicas - 1))
      (( desired < MIN )) && desired="$MIN"
      action="predictive_down ${current_replicas}->${desired}"
    else
      action="predictive_down_candidate"
    fi
  fi

  echo "$desired|$action"
}

write_header() {
  cat > "$OUT_CSV" <<'EOF'
ts,epoch_s,leader_pod,leader_ready,replica_pod_sample,replica_count_spec,replica_count_ready,replica_pods_observed,replica_pods_measured,replica_pods_baselined,leader_ops,leader_in_kbps,leader_out_kbps,leader_used_mem_bytes,leader_mem_ref_bytes,leader_mem_pct,leader_cpu_sys,leader_cpu_user,leader_connected_clients,leader_connected_slaves,replica_ops_sample,replica_ops_sum_est,replica_in_kbps_sample,replica_out_kbps_sample,replica_used_mem_bytes_sample,replica_mem_ref_bytes_sample,replica_mem_pct_sample,replica_cpu_sys_sample,replica_cpu_user_sample,replica_connected_clients_sample,replica_master_link_status_sample,replica_master_last_io_s_sample,lat_set_master_count,lat_set_master_mean_ms,lat_set_master_p50_ms,lat_set_master_p95_ms,lat_set_master_max_ms,lat_get_replica_count,lat_get_replica_mean_ms,lat_get_replica_p50_ms,lat_get_replica_p95_ms,lat_get_replica_max_ms,cpu_avg_pct,cpu_max_pct,cpu_limit_cores,current_replicas,desired_replicas,pred_enabled,pred_sequence_ready,pred_cpu_t1,pred_mem_t1,pred_reason,pred_action,pred_desired_replicas,can_scale,action,sample_count,last_scale_epoch_s,scaling_enabled
EOF
}

echo "Starting predictive-only scaler: ns=$NS dep=$REPLICA_DEPLOY interval=$INTERVAL cooldown=$COOLDOWN cpu_limit_cores=$CPU_LIMIT_CORES model=$MODEL_PATH scaler=$SCALER_PATH seq_window=$SEQ_WINDOW"
echo "writing_csv=$OUT_CSV"

write_header
init_seq_file

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
  else
    cpu_avg_pct="$(float_div "$util_sum" "$util_cnt")"
    cpu_max_pct="$util_max"
  fi

  current_replicas="$replica_spec"
  [ "$current_replicas" -le 0 ] && current_replicas="$(get_replicas)"
  desired_replicas="$current_replicas"
  action="predict_disabled"

  can_scale=0
  if (( now_s - last_scale >= COOLDOWN )); then
    can_scale=1
  fi

  append_seq_row \
    "$current_replicas" \
    "$cpu_max_pct" \
    "$replica_mem_pct_sample" \
    "$replica_in_kbps_sample" \
    "$replica_out_kbps_sample"

  sample_count=$((sample_count + 1))

  pred_enabled=0
  pred_sequence_ready=0
  pred_cpu_t1=""
  pred_mem_t1=""
  pred_reason=""
  pred_action="predict_disabled"
  pred_desired_replicas="$current_replicas"

  if [ "$ENABLE_PREDICTIVE_SCALING" = "true" ]; then
    pred_enabled=1

    if [ "$sample_count" -le "$SEQ_WINDOW" ]; then
      pred_cpu_t1="$cpu_max_pct"
      pred_mem_t1="$replica_mem_pct_sample"
      pred_reason="bootstrap_using_observed_values_row_${sample_count}_of_${SEQ_WINDOW}"

      proactive="$(select_action_predictive "${pred_cpu_t1:-0}" "$current_replicas" "$can_scale")"
      pred_desired_replicas="${proactive%%|*}"
      pred_action="${proactive#*|}"
      desired_replicas="$pred_desired_replicas"
      action="$pred_action"
    else
      pred_sequence_ready=1
      pred_json="$(predict_next_state_from_file)"

      pred_ok="$(echo "$pred_json" | "$PYTHON_BIN" -c 'import sys, json; print("1" if json.load(sys.stdin).get("ok") else "0")')"
      pred_cpu_t1="$(echo "$pred_json" | "$PYTHON_BIN" -c 'import sys, json; x = json.load(sys.stdin).get("pred_cpu_t1", ""); print(x if x != "" else "")')"
      pred_mem_t1="$(echo "$pred_json" | "$PYTHON_BIN" -c 'import sys, json; x = json.load(sys.stdin).get("pred_mem_t1", ""); print(x if x != "" else "")')"
      pred_reason="$(echo "$pred_json" | "$PYTHON_BIN" -c 'import sys, json; print(json.load(sys.stdin).get("reason", ""))')"

      if [ "$pred_ok" = "1" ]; then
        proactive="$(select_action_predictive "${pred_cpu_t1:-0}" "$current_replicas" "$can_scale")"
        pred_desired_replicas="${proactive%%|*}"
        pred_action="${proactive#*|}"
        desired_replicas="$pred_desired_replicas"
        action="$pred_action"
      else
        pred_cpu_t1="$cpu_max_pct"
        pred_mem_t1="$replica_mem_pct_sample"
        pred_reason="${pred_reason:-lstm_failed_falling_back_to_observed_values}"
        proactive="$(select_action_predictive "${pred_cpu_t1:-0}" "$current_replicas" "$can_scale")"
        pred_desired_replicas="${proactive%%|*}"
        pred_action="predict_failed_fallback"
        desired_replicas="$pred_desired_replicas"
        action="$pred_action"
      fi
    fi
  fi

  if [ "$desired_replicas" != "$current_replicas" ]; then
    if (( can_scale == 1 )); then
      if [ "$ENABLE_SCALING" = "true" ]; then
        if set_replicas "$desired_replicas"; then
          last_scale="$now_s"
        else
          desired_replicas="$current_replicas"
          pred_desired_replicas="$current_replicas"
          action="patch_failed"
          pred_action="patch_failed"
        fi
      else
        action="${action}_planned"
      fi
    else
      action="${action}_cooldown"
      desired_replicas="$current_replicas"
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

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(csv_escape "$cpu_avg_pct")" "$(csv_escape "$cpu_max_pct")" "$(csv_escape "$CPU_LIMIT_CORES")" \
    "$(csv_escape "$current_replicas")" "$(csv_escape "$desired_replicas")" "$(csv_escape "$pred_enabled")" \
    "$(csv_escape "$pred_sequence_ready")" "$(csv_escape "$pred_cpu_t1")" "$(csv_escape "$pred_mem_t1")" \
    "$(csv_escape "$pred_reason")" "$(csv_escape "$pred_action")" \
    "$(csv_escape "$pred_desired_replicas")" "$(csv_escape "$can_scale")" "$(csv_escape "$action")" \
    "$(csv_escape "$sample_count")" "$(csv_escape "$last_scale")" \
    "$(csv_escape "$([ "$ENABLE_SCALING" = "true" ] && echo 1 || echo 0)")" >> "$OUT_CSV"

  echo "{\"ts\":\"$ts\",\"action\":\"$action\",\"current\":$current_replicas,\"desired\":$desired_replicas,\"cpu_avg_pct\":$cpu_avg_pct,\"cpu_max_pct\":$cpu_max_pct,\"pred_cpu_t1\":\"$pred_cpu_t1\",\"pred_mem_t1\":\"$pred_mem_t1\",\"pred_action\":\"$pred_action\",\"pred_reason\":\"$pred_reason\",\"sample_count\":$sample_count}"

  sleep "$INTERVAL"
done