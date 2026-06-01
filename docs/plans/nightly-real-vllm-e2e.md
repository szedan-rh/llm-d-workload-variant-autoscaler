# Nightly Real-vLLM E2E Test Plan

## Context

WVA's per-PR e2e suite runs against `llm-d-inference-sim` (the simulator) on a Kind cluster.
The simulator allows injecting fake metrics (`--fake-metrics`) which makes tests fast and
deterministic. This covers WVA's internal decision logic well.

What the per-PR suite does **not** cover:

- Real vLLM startup and inference behaviour
- Real Prometheus metrics emitted by vLLM (KV cache utilisation, queue depth)
- OpenShift-specific surface: SCCs, Routes, OpenShift RBAC model
- The `config/openshift/` kustomize overlay

The upstream nightly at `llm-d/llm-d/.github/workflows/nightly-e2e-wva-ocp.yaml` already
deploys WVA on a dedicated OpenShift GPU cluster with real vLLM every night.
Currently `nightly-test-llm-d` in WVA's Makefile is a no-op stub — the nightly deploys but
runs zero tests. This plan closes that gap.

## What "real-vLLM test" means

A real-vLLM test exercises the full WVA decision loop against a live vLLM process:

```
[inference requests]
       ↓
[EPP + flow controller]  routes requests to vLLM; may throttle under heavy load
       ↓
[vLLM] emits real Prometheus metrics
   (vllm:num_requests_waiting, vllm:gpu_cache_usage_perc, …)
       ↓
[Prometheus] scrapes vLLM pods
       ↓
[WVA controller] reads metrics, runs saturation analyzer
       ↓
[WVA] writes wva_desired_replicas to Prometheus / updates VA status
       ↓
[HPA / ScaledObject] reads external metric, scales Deployment
```

The simulator short-circuits this loop by injecting pre-set metric values. A real-vLLM test
validates every link in the chain.

## Why not sustained load?

Sustained load (continuous traffic for the test duration) is non-deterministic:
vLLM throughput varies with GPU state, model warm-up, and scheduler behaviour. Saturation
may appear at different times across runs, making timing-based assertions unreliable.

The approach used here is a **controlled burst**: send enough concurrent requests to
guarantee the queue fills immediately, verify WVA detects saturation and the scaler responds,
then let the burst complete and verify scale-down. This is deterministic because:

- The nightly deploys vLLM with `--max-num-seqs=1` (see **Upstream coordination** below)
- A burst of `max_num_seqs + N` concurrent requests immediately creates a non-zero `num_requests_waiting`
- The saturation threshold is known from the deployed ConfigMap (default: `queueLengthThreshold: 5`)
- The burst size is fixed (`threshold + 2`), so queue drain time is bounded
- The flow controller does not throttle the burst below the saturation threshold (validated empirically: 10 concurrent requests reach vLLM and saturate the queue on the nightly cluster)

This mirrors the pattern already used in `scale_from_zero_test.go`, which sends a small
curl Job to trigger the zero→one transition.

> **Note**: Investigation of the upstream nightly kustomize overlay
> (`guides/optimized-baseline/modelserver/gpu/vllm/base/patch-vllm.yaml`) confirmed that
> `--max-num-seqs` is **not currently set** — vLLM uses its default (~256). With the default,
> a burst of 7 requests will not create a queue because vLLM handles all requests concurrently.
> The upstream nightly kustomize **must** be updated to add `--max-num-seqs=1` (or a similarly
> low value) before this test design is valid. This is part of the upstream coordination
> described below.

## Test design

### New Ginkgo label: `nightly`

A new `Label("nightly")` is added alongside the existing `smoke` / `full` labels. Nightly
tests may assume:
- `USE_SIMULATOR=false` (real vLLM)
- `ENVIRONMENT=openshift`
- `GATEWAY_NAME` env var is set (by the reusable workflow) and the gateway is reachable from within the cluster
- `max_num_seqs=1` (set via the upstream kustomize patch described below) so any second concurrent request immediately queues

Nightly tests must **not** use `--fake-metrics` (real vLLM rejects it).

### New test file: `nightly_saturation_test.go`

Contains two `Describe` blocks — one for each analyzer. Both share the same burst
mechanism and use `Label("nightly")`. The test file skips entirely if `USE_SIMULATOR=true`
or `ENVIRONMENT != openshift`.

**Shared setup**: the VA, HPA, and saturation ConfigMap are already deployed by
`nightly-deploy-wva-guide`. Tests target the existing InferencePool and do not create a
new VA. Gateway address is discovered from `GATEWAY_NAME` using the same cluster-lookup
pattern as the existing e2e tests (not a hardcoded URL).

**Shared burst mechanism**: submit a curl Job with `N = threshold + 2` concurrent slow
requests (long `max_tokens`). With `max_num_seqs=1`, all but one queue immediately.
The Job is cleaned up in `AfterEach` regardless of outcome.

---

#### Test 1 — V1 threshold-based analyzer (to be deprecated once V2 is fully validated)

V1 is the **default** (no `analyzerName` in ConfigMap). Scales when:
- `num_requests_waiting > queueLengthThreshold` (default: 5), or
- `kv_cache_usage_perc > kvCacheThreshold` (default: 0.80)

No ConfigMap patch needed — V1 is already active after `nightly-deploy-wva-guide`.

**Scale-up**: burst `queueLengthThreshold + 2` requests →
assert `vllm:num_requests_waiting > threshold` in Prometheus →
assert `VA.status.desiredOptimizedAlloc.numReplicas > 1` →
assert HPA scales to ≥ 2 replicas.

**Scale-down**: Job completes, queue drains →
assert `vllm:num_requests_waiting == 0` →
assert HPA returns to `minReplicas` within 3 minutes (120s stabilization + buffer).

---

#### Test 2 — V2 token-based analyzer (`analyzerName: "saturation"`)

V2 is selected by setting `analyzerName: "saturation"` in the `SaturationScalingConfig`
ConfigMap. V2 uses token-capacity modeling: it tracks `TotalKvCapacityTokens` per replica
and scales when demand exceeds available capacity.

**ConfigMap patch**: in `BeforeEach`, patch the saturation ConfigMap to add
`analyzerName: "saturation"`. Restore original ConfigMap in `AfterEach`.
Wait for WVA controller to pick up the new config (one reconcile interval) before
sending load.

**Scale-up**: same burst mechanism as V1 →
assert `VA.status.desiredOptimizedAlloc.numReplicas > 1` (V2 path) →
assert HPA scales to ≥ 2 replicas.

**Scale-down**: same as V1.

**Note**: V2 falls back gracefully when `TotalKvCapacityTokens == 0` (vLLM did not emit
`vllm:cache_config_info`). The test should assert the V2 path was actually taken by
checking `analyzer_name="saturation_analyzer_v2"` label on the `wva_config_info` metric
(set by `SetConfigInfo` on every reconcile), not just that scaling happened.

### `nightly-test-llm-d` make target

```makefile
nightly-test-llm-d:
    ENVIRONMENT=openshift \
    USE_SIMULATOR=false \
    make test-e2e-full
    ENVIRONMENT=openshift \
    USE_SIMULATOR=false \
    make test-e2e-nightly
```

`test-e2e-full` with `USE_SIMULATOR=false` runs all tests that do not require the simulator
(smoke infrastructure, scale-from-zero, limiter, annotation discovery). Simulator-only suites
(`saturation_v2_test.go`, `saturation_analyzer_path_test.go`) self-skip when `USE_SIMULATOR=false`.

`test-e2e-nightly` runs `Label("nightly")` tests — the new burst saturation test.

A new `test-e2e-nightly` Makefile target mirrors the existing pattern:
```makefile
test-e2e-nightly: ginkgo
    $(GINKGO) -v --label-filter="nightly" \
        --timeout=$(E2E_NIGHTLY_TIMEOUT) \
        $(GINKGO_FLAGS) ./test/e2e/...
```

`E2E_NIGHTLY_TIMEOUT` defaults to `60m` (nightly has no PR latency budget).

## What this covers vs. the per-PR suite

| Signal | Per-PR (simulator) | Nightly (real vLLM) |
|--------|-------------------|---------------------|
| V1 saturation decision logic | ✅ fake metrics | ✅ real metrics |
| V2 saturation decision logic | ✅ fake metrics | ✅ real metrics (ConfigMap patch) |
| Real vLLM metric emission | ❌ | ✅ |
| HPA scaling from real metrics | ❌ | ✅ |
| Scale-from-zero | ✅ | ❌ (skipped — needs KEDA; prometheus-adapter HPA on OCP nightly) |
| OpenShift SCC / Route / RBAC | ❌ | ✅ |
| `config/openshift/` kustomize overlay | ❌ | ✅ |
| Speed | Fast | Slower — expected |

## Upstream coordination (two-repo change)

PR 1b requires two changes in `llm-d/llm-d`:

**1. Enable the test target** in `nightly-e2e-wva-ocp.yaml`:
```yaml
# nightly-e2e-wva-ocp.yaml — add to the `with:` block
test_target: nightly-test-llm-d
```
Without this the reusable workflow's test step is skipped (`if: inputs.test_target`).

**2. Set `--max-num-seqs=1`** in the vLLM kustomize patch
(`guides/optimized-baseline/modelserver/gpu/vllm/base/patch-vllm.yaml`):
```yaml
- --max-num-seqs=1
```
This is required for the burst test design to work. With vLLM's default `max_num_seqs`
(~256), a burst of 7 requests is processed concurrently and never queues — `num_requests_waiting`
stays at 0 and WVA never detects saturation. With `max_num_seqs=1`, any second concurrent
request immediately enters the queue.

The WVA PR and the upstream PR should be merged together.

## Some investigations

1. **Gateway URL**: The reusable workflow sets `GATEWAY_NAME` (not a URL),
   constructed as `infra-<guide_name>-inference-gateway-istio`. No `GATEWAY_URL` env var
   exists — `gateway_host` input is ignored in the WVA test path. The nightly test must
   discover the gateway address from the cluster at test time using `GATEWAY_NAME`, following
   the same pattern as the existing e2e tests in `testconfig/config.go`.
2. **Scaler backend on OCP nightly**: The OCP overlay at
   `guides/workload-autoscaling/wva-config/platform/ocp` uses **Prometheus Adapter + standard
   HPA** (not KEDA). KEDA is not installed by the nightly deploy. The test must use
   `SCALER_BACKEND=prometheus-adapter` assertions (HPA, not ScaledObject).
3. **HPA stabilization window**: `HPA_STABILIZATION_SECONDS` is a ghost
   parameter — defined in the CI workflow input but never consumed by any deploy script or
   kustomize patch. The nightly deploys via kustomize (not Helm), so the stabilization window
   comes from `guides/workload-autoscaling/optimized-baseline-autoscaling/hpa.yaml`:
   **`stabilizationWindowSeconds: 120`** (2 min) for both scale-up and scale-down.
   The scale-down `Eventually` timeout in the nightly test should be set to at least
   **3 minutes** (120s window + polling buffer). `E2E_NIGHTLY_TIMEOUT: 60m` is sufficient.
