package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	variantautoscalingv1alpha1 "github.com/llm-d/llm-d-workload-variant-autoscaler/api/v1alpha1"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/test/e2e/fixtures"
)

// Nightly saturation tests exercise the full WVA decision loop against a live vLLM process
// on the OCP nightly cluster. They require:
//   - USE_SIMULATOR=false (real vLLM)
//   - ENVIRONMENT=openshift
//   - vLLM Deployment pre-deployed with --max-num-seqs=1 (infra deploy step, see docs/plans/nightly-real-vllm-e2e.md)
//   - WVA controller running in the same namespace
//
// The burst mechanism: N concurrent requests with high max_tokens. With max_num_seqs=1, all
// but one request immediately enter the vLLM waiting queue, pushing num_requests_waiting above
// the saturation threshold and triggering WVA to recommend a scale-up.
//
// Resource ownership:
//   - Infra deploy step owns: vLLM Deployment, ServiceAccount, gateway Service, PodMonitor
//   - These tests own: VariantAutoscaling, HorizontalPodAutoscaler, saturation ConfigMap

const (
	// nightlyQueueThreshold is the queueLengthThreshold written into the V1 saturation config by
	// BeforeAll. cfg.NightlyBurstSize must exceed this for the burst to saturate the queue.
	nightlyQueueThreshold = 5
	nightlyMaxTokens      = 1500 // keeps requests in-flight long enough for WVA to detect saturation; must be < max-model-len (2048)
	nightlyScaleDownSec   = 600  // pod model load (~4min) + WVA cycle (30s) + 300s HPA scale-down stabilization (both KEDA and prometheus-adapter) + buffer
	nightlyVariantCost    = 10.0 // GPU cost for the nightly VA; lower than the simulator default (30.0) to match OCP cluster GPU budget

	// nightlyOCPPrometheusURL is the thanos-querier endpoint used by KEDA on OpenShift.
	// KEDA queries this directly via bearer-token auth (nightlyOCPKEDATriggerAuth).
	nightlyOCPPrometheusURL   = "https://thanos-querier.openshift-monitoring.svc.cluster.local:9091"
	nightlyOCPKEDATriggerAuth = "ai-inference-keda-thanos"
)

// discoverNightlyGateway finds the inference-gateway Service name. The gateway is part of the
// infra deploy step and must exist before the test runs.
// If GATEWAY_NAME is set (injected by the llm-d-infra reusable workflow), it is used directly.
// Otherwise the namespace is scanned for any Service whose name contains "inference-gateway".
func discoverNightlyGateway() string {
	GinkgoHelper()

	if cfg.GatewayName != "" {
		GinkgoWriter.Printf("Nightly gateway: %s (from GATEWAY_NAME)\n", cfg.GatewayName)
		return cfg.GatewayName
	}

	svcs, err := k8sClient.CoreV1().Services(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred(), "should list services in llmd namespace")
	for _, svc := range svcs.Items {
		if strings.Contains(svc.Name, "inference-gateway") {
			GinkgoWriter.Printf("Nightly gateway: %s\n", svc.Name)
			return svc.Name
		}
	}
	if cfg.EPPServiceName != "" {
		return cfg.EPPServiceName
	}
	Fail("inference-gateway service not found in namespace — run the infra deploy step first")
	return ""
}

// createNightlyWVAResources creates the VariantAutoscaling and scaler (HPA or ScaledObject
// depending on cfg.ScalerBackend) owned by the nightly test suite.
func createNightlyWVAResources() {
	GinkgoHelper()
	Expect(fixtures.EnsureVariantAutoscaling(
		ctx, crClient,
		cfg.LLMDNamespace, cfg.NightlyDeployment, cfg.NightlyDeployment,
		cfg.ModelID, cfg.AcceleratorType,
		nightlyVariantCost, cfg.ControllerInstance,
	)).To(Succeed(), "creating nightly VariantAutoscaling")
	if cfg.ScalerBackend == scalerBackendKeda {
		Expect(fixtures.EnsureScaledObject(
			ctx, crClient,
			cfg.LLMDNamespace, cfg.NightlyDeployment, cfg.NightlyDeployment, cfg.NightlyDeployment,
			1, 4, cfg.MonitoringNS,
			fixtures.WithScaledObjectPrometheusServer(nightlyOCPPrometheusURL),
			fixtures.WithScaledObjectClusterTriggerAuth(nightlyOCPKEDATriggerAuth),
		)).To(Succeed(), "creating nightly ScaledObject")
	} else {
		Expect(fixtures.EnsureHPA(
			ctx, k8sClient,
			cfg.LLMDNamespace, cfg.NightlyDeployment, cfg.NightlyDeployment, cfg.NightlyDeployment,
			1, 4,
		)).To(Succeed(), "creating nightly HPA")
	}
	GinkgoWriter.Printf("Nightly resources created: va=%s scaler=%s-hpa(%s) deployment=%s\n",
		cfg.NightlyDeployment, cfg.NightlyDeployment, cfg.ScalerBackend, cfg.NightlyDeployment)
}

// deleteNightlyWVAResources removes the VA and scaler created by createNightlyWVAResources.
func deleteNightlyWVAResources() {
	if err := fixtures.DeleteVariantAutoscaling(ctx, crClient, cfg.LLMDNamespace, cfg.NightlyDeployment); err != nil {
		GinkgoWriter.Printf("Warning: failed to delete nightly VA: %v\n", err)
	}
	if cfg.ScalerBackend == scalerBackendKeda {
		if err := fixtures.DeleteScaledObject(ctx, crClient, cfg.LLMDNamespace, cfg.NightlyDeployment); err != nil {
			GinkgoWriter.Printf("Warning: failed to delete nightly ScaledObject: %v\n", err)
		}
	} else {
		if err := fixtures.DeleteHPA(ctx, k8sClient, cfg.LLMDNamespace, cfg.NightlyDeployment); err != nil {
			GinkgoWriter.Printf("Warning: failed to delete nightly HPA: %v\n", err)
		}
	}
}

// checkGPUCapacity skips the test if fewer than minReplicas GPU slots are available across
// all nodes. This prevents the test from churning hundreds of UnexpectedAdmissionError pods
// when the GPU node is unhealthy or at capacity.
//
// It compares allocatable minus requested (from non-terminal pods) for the gpu resource
// used by the vLLM deployment. A GPU in a "lost" hardware state will show as allocatable
// but the device plugin will reject pod allocation — this check won't catch that case, but
// it does catch capacity exhaustion from other workloads.
func checkGPUCapacity(deploymentName string, minReplicas int) {
	GinkgoHelper()

	// Find the GPU resource name from the target deployment's pod spec.
	dep, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		GinkgoWriter.Printf("Warning: could not read deployment %s to check GPU capacity: %v\n", deploymentName, err)
		return
	}
	var gpuResource corev1.ResourceName
	for _, c := range dep.Spec.Template.Spec.Containers {
		for rName := range c.Resources.Limits {
			if strings.Contains(string(rName), "gpu") || strings.Contains(string(rName), "GPU") {
				gpuResource = rName
				break
			}
		}
		if gpuResource != "" {
			break
		}
	}
	if gpuResource == "" {
		return // no GPU resource in deployment spec; skip capacity check
	}

	// Sum allocatable GPUs across schedulable nodes.
	nodes, err := k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred(), "listing nodes for GPU capacity check")
	var totalAllocatable int64
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			continue
		}
		tainted := false
		for _, t := range node.Spec.Taints {
			if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
				tainted = true
				break
			}
		}
		if tainted {
			continue
		}
		if q, ok := node.Status.Allocatable[gpuResource]; ok {
			totalAllocatable += q.Value()
		}
	}

	// Sum requested GPUs from non-terminal pods across all namespaces.
	allPods, err := k8sClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred(), "listing pods for GPU capacity check")
	var totalRequested int64
	for _, pod := range allPods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, c := range pod.Spec.Containers {
			if q, ok := c.Resources.Requests[gpuResource]; ok {
				totalRequested += q.Value()
			}
		}
	}

	available := totalAllocatable - totalRequested
	if available < int64(minReplicas) {
		Skip(fmt.Sprintf(
			"insufficient GPU capacity for scale-out test: need %d %s slots, have %d allocatable and %d requested (available=%d); node GPU may be lost or at capacity",
			minReplicas, gpuResource, totalAllocatable, totalRequested, available,
		))
	}
	GinkgoWriter.Printf("GPU capacity check: %s allocatable=%d requested=%d available=%d (need %d)\n",
		gpuResource, totalAllocatable, totalRequested, available, minReplicas)
}

// snapshotNightlySaturationCM captures the current saturation ConfigMap state before the test
// modifies it, so AfterAll can restore it.
func snapshotNightlySaturationCM() (name string, original *corev1.ConfigMap, existed bool) {
	GinkgoHelper()
	name = saturationConfigMapName()
	cm, err := k8sClient.CoreV1().ConfigMaps(cfg.WVANamespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return name, cm.DeepCopy(), true
	}
	if !errors.IsNotFound(err) {
		Expect(err).NotTo(HaveOccurred(), "failed reading saturation ConfigMap")
	}
	return name, nil, false
}

// restoreNightlySaturationCM restores the saturation ConfigMap to its pre-test state using the
// delete+create pattern to avoid resourceVersion conflicts (same approach as saturation_analyzer_path_test.go).
func restoreNightlySaturationCM(cmName string, cmOriginal *corev1.ConfigMap, cmExistedBefore bool) {
	propagation := metav1.DeletePropagationBackground
	if err := k8sClient.CoreV1().ConfigMaps(cfg.WVANamespace).Delete(ctx, cmName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	}); err != nil && !errors.IsNotFound(err) {
		GinkgoWriter.Printf("Warning: failed to delete saturation ConfigMap before restore: %v\n", err)
	}
	if cmExistedBefore && cmOriginal != nil {
		toCreate := saturationConfigMapForRecreate(cmOriginal)
		if _, err := k8sClient.CoreV1().ConfigMaps(cfg.WVANamespace).Create(ctx, toCreate, metav1.CreateOptions{}); err != nil {
			GinkgoWriter.Printf("Warning: failed to restore saturation ConfigMap: %v\n", err)
		}
	}
}

// deleteNightlyBurstJob removes the burst job and its pods. Safe to call in AfterEach.
func deleteNightlyBurstJob(jobName string) {
	propagation := metav1.DeletePropagationBackground
	_ = k8sClient.BatchV1().Jobs(cfg.LLMDNamespace).Delete(ctx, jobName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	_ = k8sClient.CoreV1().Pods(cfg.LLMDNamespace).DeleteCollection(ctx,
		metav1.DeleteOptions{PropagationPolicy: &propagation},
		metav1.ListOptions{LabelSelector: "job-name=" + jobName},
	)
}

// assertSaturationScaleUp submits the burst job and asserts WVA detects saturation, the HPA
// scales up to ≥ 2 replicas, and the burst job completes. Call assertSaturationScaleDown next.
func assertSaturationScaleUp(jobName, gatewayService, vaName, hpaName string) {
	GinkgoHelper()

	deleteNightlyBurstJob(jobName)
	job := createNightlySaturationBurstJob(jobName, cfg.LLMDNamespace, gatewayService, cfg.ModelID, cfg.NightlyBurstSize, nightlyMaxTokens)
	_, err := k8sClient.BatchV1().Jobs(cfg.LLMDNamespace).Create(ctx, job, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "burst job should be created")

	By("Waiting for burst job pod to start sending requests")
	Eventually(func(g Gomega) {
		pods, err := k8sClient.CoreV1().Pods(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "job-name=" + jobName,
		})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods.Items).NotTo(BeEmpty())
		phase := pods.Items[0].Status.Phase
		g.Expect(phase).To(Or(Equal(corev1.PodRunning), Equal(corev1.PodSucceeded)))
	}, time.Duration(cfg.EventuallyStandardSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

	By("Waiting for WVA to recommend ≥ 2 replicas (saturation detected)")
	Eventually(func(g Gomega) {
		va := &variantautoscalingv1alpha1.VariantAutoscaling{}
		g.Expect(crClient.Get(ctx, client.ObjectKey{Name: vaName, Namespace: cfg.LLMDNamespace}, va)).To(Succeed())
		g.Expect(va.Status.DesiredOptimizedAlloc.NumReplicas).NotTo(BeNil())
		g.Expect(*va.Status.DesiredOptimizedAlloc.NumReplicas).To(BeNumerically(">=", 2),
			"VA should recommend ≥ 2 replicas when queue is saturated")
	}, time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

	By("Waiting for HPA to desire ≥ 2 replicas (KEDA: polls every 5s, no scale-up stabilization delay; prometheus-adapter: up to 120s HPA stabilization window)")
	Eventually(func(g Gomega) {
		var desiredReplicas int32
		if cfg.ScalerBackend == scalerBackendKeda {
			hpaList, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			var found bool
			for i := range hpaList.Items {
				if hpaList.Items[i].Spec.ScaleTargetRef.Name == cfg.NightlyDeployment {
					desiredReplicas = hpaList.Items[i].Status.DesiredReplicas
					found = true
					break
				}
			}
			g.Expect(found).To(BeTrue(), "KEDA-managed HPA targeting %s not found", cfg.NightlyDeployment)
		} else {
			hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Get(ctx, hpaName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			desiredReplicas = hpa.Status.DesiredReplicas
		}
		g.Expect(desiredReplicas).To(BeNumerically(">=", 2),
			"HPA should desire ≥ 2 replicas when WVA signals saturation")
	}, time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

	By("Waiting for burst job to complete (all concurrent requests served)")
	Eventually(func(g Gomega) {
		j, err := k8sClient.BatchV1().Jobs(cfg.LLMDNamespace).Get(ctx, jobName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(j.Status.Succeeded).To(BeNumerically(">", 0))
	}, 2*time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalVerySlowSec)*time.Second).Should(Succeed())
}

// assertSaturationScaleDown asserts the deployment scales back to minReplicas after the burst.
// Must be called after assertSaturationScaleUp has completed (deployment is at ≥ 2 replicas).
//
// WVA requires nonSaturatedCount >= 2 (MinNonSaturatedReplicasForScaleDown) before approving
// scale-down. The second pod may still be loading the model; we wait for it to become Ready so
// WVA sees both pods as non-saturated on its next reconcile cycle.
func assertSaturationScaleDown(hpaName string) {
	GinkgoHelper()

	By("Waiting for scaled-up deployment to have 2 Ready replicas (WVA scale-down requires both pods non-saturated)")
	Eventually(func(g Gomega) {
		dep, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, cfg.NightlyDeployment, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(dep.Status.ReadyReplicas).To(BeNumerically(">=", 2),
			"deployment should have ≥ 2 Ready replicas before scale-down can be approved")
	}, time.Duration(cfg.PodReadyTimeout)*time.Second, time.Duration(cfg.PollIntervalSlowSec)*time.Second).Should(Succeed())

	By("Waiting for HPA to return to minReplicas after queue drains " +
		"(WVA requires ≥2 pods non-saturated; HPA 300s scale-down stabilization window applies for both KEDA and prometheus-adapter; KEDA CooldownPeriod only governs scale-to-zero)")
	Eventually(func(g Gomega) {
		minReplicas := int32(1)
		var desiredReplicas int32
		if cfg.ScalerBackend == scalerBackendKeda {
			hpaList, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			var found bool
			for i := range hpaList.Items {
				if hpaList.Items[i].Spec.ScaleTargetRef.Name == cfg.NightlyDeployment {
					if hpaList.Items[i].Spec.MinReplicas != nil {
						minReplicas = *hpaList.Items[i].Spec.MinReplicas
					}
					desiredReplicas = hpaList.Items[i].Status.DesiredReplicas
					found = true
					break
				}
			}
			g.Expect(found).To(BeTrue(), "KEDA-managed HPA targeting %s not found", cfg.NightlyDeployment)
		} else {
			hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Get(ctx, hpaName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			if hpa.Spec.MinReplicas != nil {
				minReplicas = *hpa.Spec.MinReplicas
			}
			desiredReplicas = hpa.Status.DesiredReplicas
		}
		g.Expect(desiredReplicas).To(Equal(minReplicas),
			"HPA should return to minReplicas=%d after queue drains", minReplicas)
	}, nightlyScaleDownSec*time.Second, time.Duration(cfg.PollIntervalSlowSec)*time.Second).Should(Succeed())
}

var _ = Describe("Nightly Saturation — V1 Threshold Analyzer", Label("nightly"), Ordered, func() {
	const burstJobName = "nightly-saturation-burst-v1"
	var (
		gatewayService  string
		vaName          string
		hpaName         string
		cmName          string
		cmOriginal      *corev1.ConfigMap
		cmExistedBefore bool
	)

	BeforeAll(func() {
		if cfg.UseSimulator {
			Skip("nightly saturation tests require USE_SIMULATOR=false (real vLLM)")
		}
		if cfg.Environment != environmentOpenShift {
			Skip("nightly saturation tests require ENVIRONMENT=openshift")
		}
		checkGPUCapacity(cfg.NightlyDeployment, 2)
		gatewayService = discoverNightlyGateway()
		createNightlyWVAResources()
		vaName = cfg.NightlyDeployment
		hpaName = cfg.NightlyDeployment + "-hpa"

		cmName, cmOriginal, cmExistedBefore = snapshotNightlySaturationCM()
		// nightlyQueueThreshold=5 is written into the config below; burst size must exceed it.
		Expect(cfg.NightlyBurstSize).To(BeNumerically(">", nightlyQueueThreshold),
			"E2E_NIGHTLY_BURST_SIZE must exceed queueLengthThreshold=%d to saturate the queue", nightlyQueueThreshold)
		v1Config := buildSaturationConfigYAMLWithThresholds("", 0.85, nightlyQueueThreshold, 0.10, 3, 0.85, 0.70)
		Expect(upsertSaturationConfigEntry(ctx, cfg.WVANamespace, cmName, "default", v1Config)).To(Succeed(),
			"writing V1 saturation config")
		By("Waiting for WVA to pick up V1 config")
		expectAnalyzerPathLog("V1", cfg.ModelID)
	})

	AfterAll(func() {
		deleteNightlyWVAResources()
		By("Restoring saturation ConfigMap to pre-test state")
		if cmName != "" {
			restoreNightlySaturationCM(cmName, cmOriginal, cmExistedBefore)
		}
	})

	AfterEach(func() { deleteNightlyBurstJob(burstJobName) })

	It("should scale up when queue exceeds threshold", func() {
		By("Submitting concurrent burst to saturate vLLM queue via V1 (threshold-based) analyzer")
		assertSaturationScaleUp(burstJobName, gatewayService, vaName, hpaName)
	})

	It("should scale back down after burst completes", func() {
		assertSaturationScaleDown(hpaName)
	})
})

var _ = Describe("Nightly Saturation — V2 Token Analyzer", Label("nightly"), Ordered, func() {
	const burstJobName = "nightly-saturation-burst-v2"
	var (
		gatewayService  string
		vaName          string
		hpaName         string
		cmName          string
		cmOriginal      *corev1.ConfigMap
		cmExistedBefore bool
	)

	BeforeAll(func() {
		if cfg.UseSimulator {
			Skip("nightly saturation tests require USE_SIMULATOR=false (real vLLM)")
		}
		if cfg.Environment != environmentOpenShift {
			Skip("nightly saturation tests require ENVIRONMENT=openshift")
		}
		checkGPUCapacity(cfg.NightlyDeployment, 2)
		gatewayService = discoverNightlyGateway()
		createNightlyWVAResources()
		vaName = cfg.NightlyDeployment
		hpaName = cfg.NightlyDeployment + "-hpa"

		cmName, cmOriginal, cmExistedBefore = snapshotNightlySaturationCM()
		v2Config := buildSaturationConfigYAML("saturation")
		Expect(upsertSaturationConfigEntry(ctx, cfg.WVANamespace, cmName, "default", v2Config)).To(Succeed(),
			"writing V2 saturation config")
		By("Waiting for WVA controller to pick up V2 config and log the V2 analyzer path")
		expectAnalyzerPathLog("V2", cfg.ModelID)
	})

	AfterAll(func() {
		deleteNightlyWVAResources()
		By("Restoring saturation ConfigMap to pre-test state")
		if cmName != "" {
			restoreNightlySaturationCM(cmName, cmOriginal, cmExistedBefore)
		}
	})

	AfterEach(func() { deleteNightlyBurstJob(burstJobName) })

	It("should scale up via V2 token analyzer when queue exceeds capacity", func() {
		By("Submitting concurrent burst to saturate vLLM queue via V2 analyzer")
		assertSaturationScaleUp(burstJobName, gatewayService, vaName, hpaName)
	})

	It("should scale back down after burst completes", func() {
		assertSaturationScaleDown(hpaName)
	})
})

// createNightlySaturationBurstJob creates a Job that sends burstSize concurrent requests
// with high max_tokens to force them into the vLLM waiting queue (requires --max-num-seqs=1).
func createNightlySaturationBurstJob(name, namespace, gatewayService, modelID string, burstSize, maxTokens int) *batchv1.Job {
	backoffLimit := int32(0)

	// All requests are fired in the background so they are concurrent.
	// 'wait' collects them; the job exits 0 once all finish (success or error).
	script := fmt.Sprintf(`#!/bin/sh
echo "Nightly saturation burst: %d concurrent requests to %s:80 model=%s max_tokens=%d"
N=%d
i=1
while [ $i -le $N ]; do
  curl -s --max-time 600 -X POST http://%s:80/v1/completions \
    -H "Content-Type: application/json" \
    -d '{"model":"%s","prompt":"Saturation test prompt for nightly WVA e2e. Respond in detail.","max_tokens":%d}' \
    -o /dev/null &
  i=$((i + 1))
done
echo "All $N requests dispatched. Waiting for completion..."
wait
echo "Burst complete."
`, burstSize, gatewayService, modelID, maxTokens,
		burstSize, gatewayService, modelID, maxTokens)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"test-resource": "true"},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"test-resource": "true"},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "burst-curl",
							Image:   "quay.io/curl/curl:8.11.1",
							Command: []string{"sh", "-c", script},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
						},
					},
				},
			},
		},
	}
}
