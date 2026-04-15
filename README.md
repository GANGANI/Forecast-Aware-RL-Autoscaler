# Forecast-Aware RL Autoscaler

A reinforcement learning–based autoscaling system that integrates workload forecasting to make proactive scaling decisions in cloud-native environments.

## Overview

This project demonstrates how combining **Reinforcement Learning (RL)** with **time-series forecasting (LSTM)** can improve autoscaling behavior compared to traditional reactive approaches.

The system evaluates multiple strategies:

* Reactive scaling
* Proactive scaling with LSTM.
* RL without forecasting
* RL with forecast-aware inputs (proposed approach)


---


## Repository Structure

```
.
├── data
│   ├── test/                # Evaluation results for each strategy
│   └── train/data.csv       # Training dataset (time-series system metrics)
├── models
│   ├── lstm_forecaster.pt   # Trained LSTM forecasting model
│   ├── lstm_scalers.joblib  # Data normalization scalers
│   ├── ppo_no_forecast.zip  # RL policy (no forecast)
│   └── ppo_with_forecast.zip# RL policy (forecast-aware)
└── src
    ├── experimental_setup   # Kubernetes configs (Redis microservice)
    ├── LSTM_scaler.sh       # Predictive autoscaling
    ├── reactive_scaler.sh   # Threshold-based autoscaling
    ├── RL_scaler.sh         # RL without forecast
    └── RL_LSTM_scaler.sh    # Forecast-aware RL (proposed)
```

---

## Autoscaling Strategies Implemented

The project evaluates four approaches:

1. **Reactive (Baseline)**

   * Threshold-based scaling (e.g., CPU > 80%)
   * Simple but delayed and unstable

2. **Predictive (LSTM)**

   * Uses time-series forecasting
   * Proactive but not adaptive

3. **RL (No Forecast)**

   * Learns scaling policy from interaction
   * Adaptive but reactive to current state

4. **Forecast-Aware RL (Proposed)**

   * RL + predicted workload
   * Proactive + adaptive (best overall)

---


## Running the Project

### 1. Deploy Experimental Environment

```bash
kubectl apply -f src/experimental_setup/
```

### 2. Run Autoscaling Strategies

```bash
# Reactive baseline
bash src/reactive_scaler.sh

# LSTM-based predictive scaling
bash src/LSTM_scaler.sh

# RL without forecast
bash src/RL_scaler.sh

# Forecast-aware RL (recommended)
bash src/RL_LSTM_scaler.sh
```


## Results

According to the experimental results (Table II in the paper):

| Method            | Avg Replicas | CPU (%) | p95 Latency | SLA Violations |
| ----------------- | ------------ | ------- | ----------- | -------------- |
| Reactive          | 6.5          | 42      | 245 ms      | 18.2%          |
| LSTM              | 5.3          | 58      | 195 ms      | 10.4%          |
| RL (No Forecast)  | 5.0          | 64      | 178 ms      | 7.1%           |
| **RL + Forecast** | **4.7**      | **72**  | **150 ms**  | **3.2%**       |





