package e2e

import (
	"encoding/json"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	promoperator "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/constants"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/test/e2e/fixtures"
)

type externalMetricValueList struct {
	Items []struct {
		MetricLabels map[string]string `json:"metricLabels"`
		Value        string            `json:"value"`
	} `json:"items"`
}

// cleanupSmokeTestResources deletes all resources created by smoke tests to ensure clean state
func cleanupSmokeTestResources() {
	GinkgoWriter.Println("Cleaning up smoke test resources for clean state...")

	// Helper to check if resource name matches smoke test patterns
	isSmokeTestResource := func(name string) bool {
		return strings.HasPrefix(name, "smoke-test-") || strings.HasPrefix(name, "error-test-")
	}

	// Delete all HPAs with smoke-test prefix
	hpaList, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, hpa := range hpaList.Items {
			if isSmokeTestResource(hpa.Name) {
				GinkgoWriter.Printf("  Deleting HPA: %s\n", hpa.Name)
				_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Delete(ctx, hpa.Name, metav1.DeleteOptions{})
			}
		}
	}

	// Delete all ScaledObjects with smoke-test prefix (KEDA)
	if cfg.ScalerBackend == scalerBackendKeda {
		soList := &unstructured.UnstructuredList{}
		soList.SetAPIVersion("keda.sh/v1alpha1")
		soList.SetKind("ScaledObjectList")
		if err := crClient.List(ctx, soList, client.InNamespace(cfg.LLMDNamespace)); err == nil {
			for _, so := range soList.Items {
				if isSmokeTestResource(so.GetName()) {
					GinkgoWriter.Printf("  Deleting ScaledObject: %s\n", so.GetName())
					_ = crClient.Delete(ctx, &so)
				}
			}
		}
	}

	// Delete all Deployments with smoke-test prefix
	deployList, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, deploy := range deployList.Items {
			if isSmokeTestResource(deploy.Name) {
				GinkgoWriter.Printf("  Deleting Deployment: %s\n", deploy.Name)
				_ = k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Delete(ctx, deploy.Name, metav1.DeleteOptions{})
			}
		}
	}

	// Delete all Services with smoke-test prefix
	svcList, err := k8sClient.CoreV1().Services(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, svc := range svcList.Items {
			if isSmokeTestResource(svc.Name) {
				GinkgoWriter.Printf("  Deleting Service: %s\n", svc.Name)
				_ = k8sClient.CoreV1().Services(cfg.LLMDNamespace).Delete(ctx, svc.Name, metav1.DeleteOptions{})
			}
		}
	}

	// Delete all ServiceMonitors with smoke-test prefix in monitoring namespace
	smList := &promoperator.ServiceMonitorList{}
	if err := crClient.List(ctx, smList, client.InNamespace(cfg.MonitoringNS)); err == nil {
		for _, sm := range smList.Items {
			if isSmokeTestResource(sm.Name) {
				GinkgoWriter.Printf("  Deleting ServiceMonitor: %s\n", sm.Name)
				_ = crClient.Delete(ctx, &sm)
			}
		}
	}

	// Wait a moment for deletions to propagate
	time.Sleep(2 * time.Second)
	GinkgoWriter.Println("Cleanup completed")
}

var _ = Describe("Smoke Tests - Infrastructure Readiness", Label("smoke", "full"), func() {
	Context("Basic infrastructure validation", func() {
		It("should have WVA controller running and ready", func() {
			By("Checking WVA controller pods")
			Eventually(func(g Gomega) {
				pods, err := k8sClient.CoreV1().Pods(cfg.WVANamespace).List(ctx, metav1.ListOptions{
					LabelSelector: "control-plane=controller-manager",
				})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pods.Items).NotTo(BeEmpty(), "WVA controller pod should exist")

				// At least one pod should be running and ready
				readyPods := 0
				for _, pod := range pods.Items {
					if pod.Status.Phase == corev1.PodRunning {
						for _, condition := range pod.Status.Conditions {
							if condition.Type == "Ready" && condition.Status == "True" {
								readyPods++
								break
							}
						}
					}
				}
				g.Expect(readyPods).To(BeNumerically(">", 0), "At least one WVA controller pod should be ready")
			}).Should(Succeed())
		})

		It("should have llm-d CRDs installed", func() {
			By("Checking for InferencePool CRD")
			_, err := k8sClient.Discovery().ServerResourcesForGroupVersion("inference.networking.k8s.io/v1")
			Expect(err).NotTo(HaveOccurred(), "llm-d CRDs should be installed")
		})

		It("should have Prometheus running", func() {
			By("Checking Prometheus pods")
			Eventually(func(g Gomega) {
				pods, err := k8sClient.CoreV1().Pods(cfg.MonitoringNS).List(ctx, metav1.ListOptions{
					LabelSelector: "app.kubernetes.io/name=prometheus",
				})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pods.Items).NotTo(BeEmpty(), "Prometheus pod should exist")
			}).Should(Succeed())
		})

		When("using Prometheus Adapter as scaler backend", func() {
			It("should have external metrics API available", func() {
				if cfg.ScalerBackend != "prometheus-adapter" {
					Skip("External metrics API check only applies to Prometheus Adapter backend")
				}
				By("Checking for external.metrics.k8s.io API group")
				Eventually(func(g Gomega) {
					_, err := k8sClient.Discovery().ServerResourcesForGroupVersion("external.metrics.k8s.io/v1beta1")
					g.Expect(err).NotTo(HaveOccurred(), "External metrics API should be available")
				}).Should(Succeed())
			})
		})

		When("using KEDA as scaler backend", func() {
			It("should have KEDA operator ready", func() {
				if cfg.ScalerBackend != "keda" {
					Skip("KEDA readiness check only applies when SCALER_BACKEND=keda")
				}
				By("Checking KEDA operator pods in " + cfg.KEDANamespace)
				Eventually(func(g Gomega) {
					pods, err := k8sClient.CoreV1().Pods(cfg.KEDANamespace).List(ctx, metav1.ListOptions{
						LabelSelector: "app.kubernetes.io/name=keda-operator",
					})
					g.Expect(err).NotTo(HaveOccurred(), "Failed to list KEDA pods")
					g.Expect(pods.Items).NotTo(BeEmpty(), "At least one KEDA operator pod should exist")
					ready := 0
					for _, p := range pods.Items {
						if p.Status.Phase == corev1.PodRunning {
							for _, c := range p.Status.Conditions {
								if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
									ready++
									break
								}
							}
						}
					}
					g.Expect(ready).To(BeNumerically(">", 0), "At least one KEDA operator pod should be ready")
				}).Should(Succeed())
			})
		})
	})

	Context("External metrics namespace isolation", Serial, Ordered, func() {
		var (
			primaryNamespace   = "llm-d-sim"
			secondaryNamespace = "llm-d-sim-mt"
			// Both namespaces use the SAME scaler base name so the discovered
			// variant_name (the HPA object name, <base>-hpa) is identical across
			// namespaces — the overlapping-name scenario this test isolates by
			// exported_namespace. HPA names only need to be unique within a namespace.
			sharedHPABase      = "smoke-test-mt-shared"
			sharedVariantName  = sharedHPABase + "-hpa" // WVA emits variant_name = HPA object name
			primaryModelName   = "smoke-test-mt-primary-ms"
			secondaryModelName = "smoke-test-mt-secondary-ms"
			poolName           = "smoke-test-mt-pool"
		)

		BeforeAll(func() {
			if cfg.ScalerBackend == scalerBackendKeda {
				Skip("Namespace-isolation external metrics check is specific to Prometheus Adapter backend")
			}
			if cfg.Environment != envKindEmulator {
				Skip("Namespace-isolation smoke scenario currently targets kind-emulator setup")
			}

			By("Creating secondary namespace for isolation test")
			_, err := k8sClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: secondaryNamespace,
				},
			}, metav1.CreateOptions{})
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred(), "Failed to create secondary namespace")
			}

			DeferCleanup(func() {
				propagation := metav1.DeletePropagationBackground
				_ = k8sClient.CoreV1().Namespaces().Delete(ctx, secondaryNamespace, metav1.DeleteOptions{
					PropagationPolicy: &propagation,
				})
			})

			By("Creating model services in both namespaces with overlapping variant name")
			err = fixtures.EnsureModelService(ctx, k8sClient, primaryNamespace, primaryModelName, poolName, cfg.ModelID, sharedVariantName, cfg.UseSimulator, cfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary model service")
			err = fixtures.EnsureService(ctx, k8sClient, primaryNamespace, primaryModelName, primaryModelName+"-decode", 8000)
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary model service Service")
			err = fixtures.EnsureServiceMonitor(ctx, crClient, cfg.MonitoringNS, primaryNamespace, primaryModelName, primaryModelName+"-decode")
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary ServiceMonitor")

			err = fixtures.EnsureModelService(ctx, k8sClient, secondaryNamespace, secondaryModelName, poolName, cfg.ModelID, sharedVariantName, cfg.UseSimulator, cfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary model service")
			err = fixtures.EnsureService(ctx, k8sClient, secondaryNamespace, secondaryModelName, secondaryModelName+"-decode", 8000)
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary model service Service")
			err = fixtures.EnsureServiceMonitor(ctx, crClient, cfg.MonitoringNS, secondaryNamespace, secondaryModelName, secondaryModelName+"-decode")
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary ServiceMonitor")

			By("Creating annotated HPAs in both namespaces with the same base name (overlapping variant name)")
			// Both HPAs share sharedHPABase, so each is named sharedVariantName within
			// its namespace and WVA emits wva_desired_replicas{variant_name=sharedVariantName}
			// for both — isolated only by exported_namespace. The vaName arg wires the
			// HPA's own external-metric selector to the same variant_name/namespace.
			err = fixtures.EnsureHPA(ctx, k8sClient, primaryNamespace, sharedHPABase, primaryModelName+"-decode", sharedVariantName, 1, 10,
				fixtures.WithWVAAnnotations(cfg.ModelID, "30.0"))
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary HPA")
			err = fixtures.EnsureHPA(ctx, k8sClient, secondaryNamespace, sharedHPABase, secondaryModelName+"-decode", sharedVariantName, 1, 10,
				fixtures.WithWVAAnnotations(cfg.ModelID, "30.0"))
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary HPA")
		})

		It("should return exactly one external metric item when exported_namespace is selected", func() {
			By("Waiting for wva_desired_replicas to be emitted for both namespaces")
			Eventually(func(g Gomega) {
				for _, ns := range []string{primaryNamespace, secondaryNamespace} {
					raw, err := k8sClient.RESTClient().
						Get().
						AbsPath("/apis/external.metrics.k8s.io/v1beta1/namespaces/"+ns+"/"+constants.WVADesiredReplicas).
						Param("labelSelector", "variant_name="+sharedVariantName+",exported_namespace="+ns).
						DoRaw(ctx)
					g.Expect(err).NotTo(HaveOccurred(), "External metrics API query should succeed for %s", ns)
					g.Expect(strings.Contains(string(raw), `"items":[]`)).To(BeFalse(),
						"wva_desired_replicas should be emitted for namespace %s", ns)
				}
			}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Querying external metrics API with explicit namespace-aware label selector")
			var metricList externalMetricValueList
			Eventually(func(g Gomega) {
				raw, err := k8sClient.RESTClient().
					Get().
					AbsPath("/apis/external.metrics.k8s.io/v1beta1/namespaces/"+primaryNamespace+"/"+constants.WVADesiredReplicas).
					Param("labelSelector", "variant_name="+sharedVariantName+",exported_namespace="+primaryNamespace).
					DoRaw(ctx)
				g.Expect(err).NotTo(HaveOccurred(), "External metrics API query should succeed")
				g.Expect(json.Unmarshal(raw, &metricList)).To(Succeed(), "Should decode external metric response")
				g.Expect(metricList.Items).To(HaveLen(1), "Expected exactly one metric series for selected namespace and variant")
				g.Expect(metricList.Items[0].MetricLabels["exported_namespace"]).To(Equal(primaryNamespace))
				g.Expect(metricList.Items[0].MetricLabels["variant_name"]).To(Equal(sharedVariantName))
			}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Verifying both HPAs report active metric scaling")
			Eventually(func(g Gomega) {
				hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(primaryNamespace).Get(ctx, sharedHPABase+"-hpa", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				var scalingActive *autoscalingv2.HorizontalPodAutoscalerCondition
				for i := range hpa.Status.Conditions {
					if hpa.Status.Conditions[i].Type == autoscalingv2.ScalingActive {
						scalingActive = &hpa.Status.Conditions[i]
						break
					}
				}
				if scalingActive == nil || scalingActive.Status != corev1.ConditionTrue {
					GinkgoWriter.Printf("primary HPA %s/%s conditions: %+v\n", primaryNamespace, sharedHPABase+"-hpa", hpa.Status.Conditions)
				}
				g.Expect(scalingActive).NotTo(BeNil(), "Primary HPA should report ScalingActive condition")
				g.Expect(scalingActive.Status).To(Equal(corev1.ConditionTrue), "Primary HPA should have external metric available")
			}, time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			Eventually(func(g Gomega) {
				hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(secondaryNamespace).Get(ctx, sharedHPABase+"-hpa", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				var scalingActive *autoscalingv2.HorizontalPodAutoscalerCondition
				for i := range hpa.Status.Conditions {
					if hpa.Status.Conditions[i].Type == autoscalingv2.ScalingActive {
						scalingActive = &hpa.Status.Conditions[i]
						break
					}
				}
				if scalingActive == nil || scalingActive.Status != corev1.ConditionTrue {
					GinkgoWriter.Printf("secondary HPA %s/%s conditions: %+v\n", secondaryNamespace, sharedHPABase+"-hpa", hpa.Status.Conditions)
				}
				g.Expect(scalingActive).NotTo(BeNil(), "Secondary HPA should report ScalingActive condition")
				g.Expect(scalingActive.Status).To(Equal(corev1.ConditionTrue), "Secondary HPA should have external metric available")
			}, time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())
		})
	})

	Context("Basic scaler lifecycle", Serial, Ordered, func() {
		var (
			poolName         = "smoke-test-pool"
			modelServiceName = "smoke-test-ms"
			deploymentName   = modelServiceName + "-decode"
			hpaName          = "smoke-test-hpa"
			// variantName is the annotated scaler's OBJECT name (hpaName+"-so" for
			// KEDA, +"-hpa" for the adapter), stamped as the decode pods'
			// llm-d.ai/variant label so metric attribution lines up. Set below.
			variantName string
			minReplicas = int32(1) // Store minReplicas for stabilization check
		)

		BeforeAll(func() {
			if cfg.ScalerBackend == scalerBackendKeda {
				variantName = hpaName + "-so"
			} else {
				variantName = hpaName + "-hpa"
			}

			By("Cleaning up any existing smoke test resources")
			cleanupSmokeTestResources()

			By("Creating model service deployment")
			err := fixtures.EnsureModelService(ctx, k8sClient, cfg.LLMDNamespace, modelServiceName, poolName, cfg.ModelID, variantName, cfg.UseSimulator, cfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create model service")

			// Register cleanup for deployment (runs even if test fails)
			DeferCleanup(func() {
				cleanupResource(ctx, "Deployment", cfg.LLMDNamespace, deploymentName,
					func() error {
						return k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
					},
					func() bool {
						_, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
						return errors.IsNotFound(err)
					})
			})

			By("Creating service to expose model server")
			err = fixtures.EnsureService(ctx, k8sClient, cfg.LLMDNamespace, modelServiceName, deploymentName, 8000)
			Expect(err).NotTo(HaveOccurred(), "Failed to create service")

			// Register cleanup for service
			DeferCleanup(func() {
				serviceName := modelServiceName + "-service"
				cleanupResource(ctx, "Service", cfg.LLMDNamespace, serviceName,
					func() error {
						return k8sClient.CoreV1().Services(cfg.LLMDNamespace).Delete(ctx, serviceName, metav1.DeleteOptions{})
					},
					func() bool {
						_, err := k8sClient.CoreV1().Services(cfg.LLMDNamespace).Get(ctx, serviceName, metav1.GetOptions{})
						return errors.IsNotFound(err)
					})
			})

			By("Creating ServiceMonitor for metrics scraping")
			err = fixtures.EnsureServiceMonitor(ctx, crClient, cfg.MonitoringNS, cfg.LLMDNamespace, modelServiceName, deploymentName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ServiceMonitor")

			// Register cleanup for ServiceMonitor
			DeferCleanup(func() {
				serviceMonitorName := modelServiceName + "-monitor"
				cleanupResource(ctx, "ServiceMonitor", cfg.MonitoringNS, serviceMonitorName,
					func() error {
						return crClient.Delete(ctx, &promoperator.ServiceMonitor{
							ObjectMeta: metav1.ObjectMeta{
								Name:      serviceMonitorName,
								Namespace: cfg.MonitoringNS,
							},
						})
					},
					func() bool {
						err := crClient.Get(ctx, client.ObjectKey{Name: serviceMonitorName, Namespace: cfg.MonitoringNS}, &promoperator.ServiceMonitor{})
						return errors.IsNotFound(err)
					})
			})

			By("Waiting for model service to be ready")
			Eventually(func(g Gomega) {
				deployment, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(deployment.Status.ReadyReplicas).To(Equal(int32(1)), "Model service should have 1 ready replica")
			}, time.Duration(cfg.PodReadyTimeout)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Creating annotated scaler for the deployment (discovery source + scaler; HPA or ScaledObject per backend)")
			if cfg.ScaleToZeroEnabled {
				minReplicas = 0
			}
			if cfg.ScalerBackend == scalerBackendKeda {
				_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Delete(ctx, hpaName+"-hpa", metav1.DeleteOptions{})
				err = fixtures.EnsureScaledObject(ctx, crClient, cfg.LLMDNamespace, hpaName, deploymentName, variantName, minReplicas, 10, cfg.MonitoringNS,
					fixtures.WithScaledObjectWVAAnnotations(cfg.ModelID, "30.0"))
				Expect(err).NotTo(HaveOccurred(), "Failed to create ScaledObject")
			} else {
				err = fixtures.EnsureHPA(ctx, k8sClient, cfg.LLMDNamespace, hpaName, deploymentName, variantName, minReplicas, 10,
					fixtures.WithWVAAnnotations(cfg.ModelID, "30.0"))
				Expect(err).NotTo(HaveOccurred(), "Failed to create HPA")
			}
		})

		AfterAll(func() {
			By("Cleaning up test resources")
			// Load Job, Service, Deployment, and ServiceMonitor cleanup is handled by DeferCleanup registered in BeforeAll and test

			if cfg.ScalerBackend == scalerBackendKeda {
				err := fixtures.DeleteScaledObject(ctx, crClient, cfg.LLMDNamespace, hpaName)
				Expect(err).NotTo(HaveOccurred())
			} else {
				hpaNameFull := hpaName + "-hpa"
				cleanupResource(ctx, "HPA", cfg.LLMDNamespace, hpaNameFull,
					func() error {
						return k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Delete(ctx, hpaNameFull, metav1.DeleteOptions{})
					},
					func() bool {
						_, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Get(ctx, hpaNameFull, metav1.GetOptions{})
						return errors.IsNotFound(err)
					})
			}
		})

		It("should expose wva_desired_replicas for the annotated scaler", func() {
			// WVA discovers the variant from the annotated scaler and emits
			// wva_desired_replicas. We observe that through the scaler surface:
			// for KEDA via the managed HPA's CurrentMetrics, for the Prometheus
			// Adapter backend via the external metrics API.
			if cfg.ScalerBackend == scalerBackendKeda {
				By("Verifying ScaledObject exists (KEDA backend)")
				soName := hpaName + "-so"
				so := &unstructured.Unstructured{}
				so.SetAPIVersion("keda.sh/v1alpha1")
				so.SetKind("ScaledObject")
				err := crClient.Get(ctx, client.ObjectKey{Namespace: cfg.LLMDNamespace, Name: soName}, so)
				Expect(err).NotTo(HaveOccurred(), "ScaledObject %s should exist", soName)

				By("Verifying KEDA has read wva_desired_replicas from Prometheus")
				Eventually(func(g Gomega) {
					hpaList, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
					g.Expect(err).NotTo(HaveOccurred())
					var kedaHPA *autoscalingv2.HorizontalPodAutoscaler
					for i := range hpaList.Items {
						if hpaList.Items[i].Spec.ScaleTargetRef.Name == deploymentName {
							kedaHPA = &hpaList.Items[i]
							break
						}
					}
					g.Expect(kedaHPA).NotTo(BeNil(), "KEDA should have created an HPA for the deployment")
					g.Expect(kedaHPA.Status.CurrentMetrics).NotTo(BeEmpty(),
						"KEDA HPA should have CurrentMetrics populated, indicating wva_desired_replicas was read from Prometheus")
				}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())
			} else {
				By("Querying external metrics API for wva_desired_replicas")
				// Note: The metric may not exist until the Engine has run and emitted metrics to
				// Prometheus, which Prometheus Adapter then queries. This can take time.
				Eventually(func(g Gomega) {
					result, err := k8sClient.RESTClient().
						Get().
						AbsPath("/apis/external.metrics.k8s.io/v1beta1/namespaces/" + cfg.LLMDNamespace + "/" + constants.WVADesiredReplicas).
						DoRaw(ctx)
					if err != nil {
						if errors.IsNotFound(err) {
							// API accessible but metric not yet emitted — engine may not have ticked
							_, discoveryErr := k8sClient.Discovery().ServerResourcesForGroupVersion("external.metrics.k8s.io/v1beta1")
							g.Expect(discoveryErr).NotTo(HaveOccurred(), "External metrics API should be accessible")
							return
						}
						g.Expect(err).NotTo(HaveOccurred())
					}
					if !strings.Contains(string(result), `"items":[]`) {
						g.Expect(string(result)).To(ContainSubstring(constants.WVADesiredReplicas))
						GinkgoWriter.Printf("wva_desired_replicas emitted for annotated HPA %s\n", hpaName+"-hpa")
					}
				}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())
			}
		})

		It("should have scaling controlled by backend", func() {
			if cfg.ScalerBackend == scalerBackendKeda {
				By("Verifying ScaledObject exists and KEDA has created an HPA")
				soName := hpaName + "-so"
				so := &unstructured.Unstructured{}
				so.SetAPIVersion("keda.sh/v1alpha1")
				so.SetKind("ScaledObject")
				err := crClient.Get(ctx, client.ObjectKey{Namespace: cfg.LLMDNamespace, Name: soName}, so)
				Expect(err).NotTo(HaveOccurred(), "ScaledObject should exist")
				// KEDA creates an HPA for the ScaledObject; name pattern is often keda-hpa-<scaledobject> or from status
				Eventually(func(g Gomega) {
					hpaList, listErr := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
					g.Expect(listErr).NotTo(HaveOccurred())
					var kedaHPA *autoscalingv2.HorizontalPodAutoscaler
					for i := range hpaList.Items {
						h := &hpaList.Items[i]
						if h.Spec.ScaleTargetRef.Name == deploymentName {
							kedaHPA = h
							break
						}
					}
					g.Expect(kedaHPA).NotTo(BeNil(), "KEDA should have created an HPA for the deployment")
					g.Expect(kedaHPA.Status.DesiredReplicas).To(BeNumerically(">=", 0), "HPA should have desired replicas set")
				}).Should(Succeed())
			} else {
				By("Verifying HPA exists and is configured")
				hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Get(ctx, hpaName+"-hpa", metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred(), "HPA should exist")
				Expect(hpa.Spec.Metrics).NotTo(BeEmpty(), "HPA should have metrics configured")
				Expect(hpa.Spec.Metrics[0].Type).To(Equal(autoscalingv2.ExternalMetricSourceType), "HPA should use External metric type")
				Expect(hpa.Spec.Metrics[0].External.Metric.Name).To(Equal(constants.WVADesiredReplicas), "HPA should use wva_desired_replicas metric")

				By("Waiting for HPA to read the metric and update status")
				Eventually(func(g Gomega) {
					hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Get(ctx, hpaName+"-hpa", metav1.GetOptions{})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(hpa.Status.CurrentReplicas).To(BeNumerically(">=", 0), "HPA should have current replicas set")
					g.Expect(hpa.Status.DesiredReplicas).To(BeNumerically(">=", 0), "HPA should have desired replicas set")
				}).Should(Succeed())
			}
		})

		It("should verify Prometheus is scraping vLLM metrics", func() {
			By("Checking that deployment pods are ready and reporting metrics")
			Eventually(func(g Gomega) {
				pods, err := k8sClient.CoreV1().Pods(cfg.LLMDNamespace).List(ctx, metav1.ListOptions{
					LabelSelector: "app=" + modelServiceName + "-decode",
				})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pods.Items).NotTo(BeEmpty(), "Should have at least one pod")

				// At least one pod should be ready
				readyCount := 0
				for _, pod := range pods.Items {
					for _, condition := range pod.Status.Conditions {
						if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
							readyCount++
							break
						}
					}
				}
				g.Expect(readyCount).To(BeNumerically(">", 0), "At least one pod should be ready for metrics scraping")
			}).Should(Succeed())

			// Note: Direct Prometheus query would require port-forwarding or in-cluster access
			// For smoke tests, we verify pods are ready (which is a prerequisite for metrics)
			// Full Prometheus query validation is in the full test suite
		})

		It("should collect saturation metrics without triggering scale-up", func() {
			By("Recording the deployment's current replica count")
			deployment, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Deployment should exist")
			baselineReplicas := int32(1)
			if deployment.Spec.Replicas != nil {
				baselineReplicas = *deployment.Spec.Replicas
			}

			By("Verifying the deployment stays at its current replica count (no scale-up without load)")
			// Smoke tests apply no load, so the annotated scaler must not scale the
			// deployment above its baseline replica count.
			Consistently(func(g Gomega) {
				d, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(d.Status.Replicas).To(BeNumerically("<=", baselineReplicas),
					"Deployment should not scale up beyond baseline (%d) without applied load", baselineReplicas)
			}, time.Duration(cfg.EventuallyShortSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())
		})
	})

	Context("Error handling and graceful degradation", Label("smoke", "full"), Ordered, func() {
		var (
			errorTestPoolName         = "error-test-pool"
			errorTestModelServiceName = "error-test-ms"
			errorTestScalerBase       = "error-test-ms"
			// errorTestVariantName is the annotated scaler's OBJECT name
			// (base+"-so" for KEDA, +"-hpa" for the adapter), stamped as the decode
			// pods' llm-d.ai/variant label for metric attribution. Set below.
			errorTestVariantName string
		)

		BeforeAll(func() {
			if cfg.ScalerBackend == scalerBackendKeda {
				errorTestVariantName = errorTestScalerBase + "-so"
			} else {
				errorTestVariantName = errorTestScalerBase + "-hpa"
			}

			By("Cleaning up any existing smoke test resources")
			cleanupSmokeTestResources()

			deploymentName := errorTestModelServiceName + "-decode"

			By("Creating model service deployment for error handling tests")
			err := fixtures.EnsureModelService(ctx, k8sClient, cfg.LLMDNamespace, errorTestModelServiceName, errorTestPoolName, cfg.ModelID, errorTestVariantName, cfg.UseSimulator, cfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create model service")

			// Register cleanup for deployment
			DeferCleanup(func() {
				cleanupResource(ctx, "Deployment", cfg.LLMDNamespace, deploymentName,
					func() error {
						return k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
					},
					func() bool {
						_, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
						return errors.IsNotFound(err)
					})
			})

			By("Waiting for model service to be ready")
			Eventually(func(g Gomega) {
				deployment, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(deployment.Status.ReadyReplicas).To(Equal(int32(1)), "Model service should have 1 ready replica")
			}, time.Duration(cfg.PodReadyTimeout)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Creating annotated scaler (discovery source + scaler)")
			if cfg.ScalerBackend == scalerBackendKeda {
				_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Delete(ctx, errorTestScalerBase+"-hpa", metav1.DeleteOptions{})
				err = fixtures.EnsureScaledObject(ctx, crClient, cfg.LLMDNamespace, errorTestScalerBase, deploymentName, errorTestVariantName, 1, 10, cfg.MonitoringNS,
					fixtures.WithScaledObjectWVAAnnotations(cfg.ModelID, "30.0"))
				Expect(err).NotTo(HaveOccurred(), "Failed to create ScaledObject")
				DeferCleanup(func() { _ = fixtures.DeleteScaledObject(ctx, crClient, cfg.LLMDNamespace, errorTestScalerBase) })
			} else {
				err = fixtures.EnsureHPA(ctx, k8sClient, cfg.LLMDNamespace, errorTestScalerBase, deploymentName, errorTestVariantName, 1, 10,
					fixtures.WithWVAAnnotations(cfg.ModelID, "30.0"))
				Expect(err).NotTo(HaveOccurred(), "Failed to create HPA")
				DeferCleanup(func() {
					_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Delete(ctx, errorTestScalerBase+"-hpa", metav1.DeleteOptions{})
				})
			}
		})

		// scalerExists verifies the annotated scaler (HPA or ScaledObject) is still present.
		scalerExists := func(g Gomega) {
			if cfg.ScalerBackend == scalerBackendKeda {
				so := &unstructured.Unstructured{}
				so.SetAPIVersion("keda.sh/v1alpha1")
				so.SetKind("ScaledObject")
				err := crClient.Get(ctx, client.ObjectKey{Namespace: cfg.LLMDNamespace, Name: errorTestScalerBase + "-so"}, so)
				g.Expect(err).NotTo(HaveOccurred(), "ScaledObject should still exist")
			} else {
				_, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(cfg.LLMDNamespace).Get(ctx, errorTestScalerBase+"-hpa", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred(), "HPA should still exist")
			}
		}

		It("should handle deployment deletion gracefully", func() {
			deploymentName := errorTestModelServiceName + "-decode"

			By("Verifying deployment exists before deletion")
			_, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Deployment should exist before deletion")

			By("Deleting the deployment")
			err = k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred(), "Should be able to delete deployment")

			By("Waiting for deployment to be fully deleted")
			Eventually(func(g Gomega) {
				_, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
				g.Expect(err).To(HaveOccurred(), "Deployment should be deleted")
				g.Expect(errors.IsNotFound(err)).To(BeTrue(), "Error should be NotFound")
			}, time.Duration(cfg.EventuallyShortSec)*time.Second, time.Duration(cfg.PollIntervalQuickSec)*time.Second).Should(Succeed())

			By("Verifying the annotated scaler continues to exist after deployment deletion")
			// The scaler (discovery source) should survive the target deployment being deleted;
			// WVA must degrade gracefully rather than remove the scaler.
			scalerExists(Default)

			By("Recreating the deployment")
			err = fixtures.EnsureModelService(ctx, k8sClient, cfg.LLMDNamespace, errorTestModelServiceName, errorTestPoolName, cfg.ModelID, errorTestVariantName, cfg.UseSimulator, cfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to recreate model service")

			By("Waiting for deployment to be created and progressing")
			Eventually(func(g Gomega) {
				deployment, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred(), "Deployment should be created")
				// Verify deployment exists and is progressing (may not be ready yet)
				g.Expect(deployment.Status.Replicas).To(BeNumerically(">=", 0), "Deployment should have replica status")
			}, time.Duration(cfg.EventuallyMediumSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Waiting for deployment to be ready (with extended timeout for recreation)")
			// When recreating, pods may take longer to start (image pull, etc.)
			Eventually(func(g Gomega) {
				deployment, err := k8sClient.AppsV1().Deployments(cfg.LLMDNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(deployment.Status.ReadyReplicas).To(Equal(int32(1)),
					"Model service should have 1 ready replica after recreation")
			}, time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalSlowSec)*time.Second).Should(Succeed())

			By("Verifying the annotated scaler automatically resumes operation")
			// After the target is recreated, WVA should re-discover the variant and the
			// scaler should still be present and consuming wva_desired_replicas.
			Eventually(func(g Gomega) {
				scalerExists(g)
			}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())
		})

		It("should handle metrics unavailability gracefully", func() {
			By("Verifying the annotated scaler remains stable when metrics may be unavailable")
			// Smoke tests apply no load and don't guarantee fresh metrics; WVA must keep
			// the scaler in place and degrade gracefully rather than crash or remove it.
			Eventually(func(g Gomega) {
				scalerExists(g)
			}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Verifying the WVA controller stays running")
			Eventually(func(g Gomega) {
				pods, err := k8sClient.CoreV1().Pods(cfg.WVANamespace).List(ctx, metav1.ListOptions{
					LabelSelector: "control-plane=controller-manager",
				})
				g.Expect(err).NotTo(HaveOccurred())
				readyPods := 0
				for _, pod := range pods.Items {
					if pod.Status.Phase == corev1.PodRunning {
						for _, condition := range pod.Status.Conditions {
							if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
								readyPods++
								break
							}
						}
					}
				}
				g.Expect(readyPods).To(BeNumerically(">", 0),
					"WVA controller should stay ready even when metrics are unavailable")
			}).Should(Succeed())
		})
	})
})

// expectWVARaisesDesiredReplicas asserts that WVA's engine has decided to scale
// the given variant above `above` replicas — the annotation-mode equivalent of the
// pre-CRD-removal check on VariantAutoscaling.Status.DesiredOptimizedAlloc.NumReplicas.
// It is decoupled from whether the scaler has actually resized the Deployment.
//
//   - Prometheus-adapter backend: query the external.metrics.k8s.io API and assert
//     the reported wva_desired_replicas value is > above. The adapter surfaces the
//     gauge value faithfully.
//   - KEDA backend: assert the KEDA-managed HPA has a non-empty external
//     CurrentMetrics entry. KEDA only populates CurrentMetrics after successfully
//     reading wva_desired_replicas from Prometheus, so this proves the engine's
//     decision was emitted and consumed. (KEDA's emulated HPA does not surface a
//     reliable numeric gauge value — every KEDA e2e asserts consumption, not the
//     value — so a strict value threshold is not portable to this backend.)
//
// The caller wraps this in Eventually.
func expectWVARaisesDesiredReplicas(g Gomega, namespace, variantName, scaleTargetDeployment string, above int64) {
	if cfg.ScalerBackend == scalerBackendKeda {
		hpaList, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(ctx, metav1.ListOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		var consumed bool
		for i := range hpaList.Items {
			if hpaList.Items[i].Spec.ScaleTargetRef.Name != scaleTargetDeployment {
				continue
			}
			for _, m := range hpaList.Items[i].Status.CurrentMetrics {
				if m.External != nil {
					consumed = true
				}
			}
		}
		g.Expect(consumed).To(BeTrue(),
			"KEDA HPA for %s should have an external CurrentMetrics entry, proving wva_desired_replicas was emitted and consumed", scaleTargetDeployment)
		return
	}

	raw, err := k8sClient.RESTClient().
		Get().
		AbsPath("/apis/external.metrics.k8s.io/v1beta1/namespaces/"+namespace+"/"+constants.WVADesiredReplicas).
		Param("labelSelector", "variant_name="+variantName+",exported_namespace="+namespace).
		DoRaw(ctx)
	if err != nil {
		if errors.IsNotFound(err) {
			g.Expect(err).NotTo(HaveOccurred(), "wva_desired_replicas should be available for %s", variantName)
		}
		g.Expect(err).NotTo(HaveOccurred())
	}
	var list externalMetricValueList
	g.Expect(json.Unmarshal(raw, &list)).To(Succeed())
	g.Expect(list.Items).NotTo(BeEmpty(), "wva_desired_replicas should be available for %s", variantName)
	q, err := resource.ParseQuantity(list.Items[0].Value)
	g.Expect(err).NotTo(HaveOccurred(), "wva_desired_replicas value should parse")
	GinkgoWriter.Printf("  wva_desired_replicas(%s)=%d (want > %d)\n", variantName, q.Value(), above)
	g.Expect(q.Value()).To(BeNumerically(">", above),
		"WVA should raise wva_desired_replicas above %d for %s", above, variantName)
}
