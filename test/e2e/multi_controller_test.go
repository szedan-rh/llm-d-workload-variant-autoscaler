package e2e

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/constants"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/test/e2e/fixtures"
)

const secondaryControllerOverlayPathEnv = "WVA_E2E_SECONDARY_OVERLAY_PATH"

func splitImage(image string) (string, string) {
	lastColon := strings.LastIndex(image, ":")
	lastSlash := strings.LastIndex(image, "/")
	if lastColon == -1 || lastColon < lastSlash {
		return image, "latest"
	}
	return image[:lastColon], image[lastColon+1:]
}

var _ = Describe("Multi-controller Tests - Dual namespace-scoped isolation", Label("multi-controller"), func() {
	// TODO: run dual-controller isolation in a dedicated fresh cluster rather than layering
	// a namespace-scoped secondary controller on top of an existing cluster-scoped primary.
	// The two modes are mutually exclusive by design: cluster-scoped ClusterRoleBindings
	// (metrics-auth-rolebinding, manager-rolebinding, epp-metrics-reader-role-binding) are
	// shared resources and get overwritten by each kustomize apply, requiring fragile
	// patch-and-restore workarounds. A proper fix is a separate Kind cluster per scenario.
	Context("Dual namespace-scoped controllers isolation", Serial, Ordered, func() {
		var (
			primaryNamespace    = "llm-d-sim"
			secondaryNamespace  = "llm-d-sim-dual"
			secondaryController = "workload-variant-autoscaler-system-dual"
			// Deliberately identical HPA object name in both namespaces to prove
			// isolation works despite overlapping (colliding) variant names. The HPA
			// object name is also the variant_name label on wva_desired_replicas.
			sharedHPABase      = "smoke-test-dual-shared"
			sharedVariantName  = sharedHPABase + "-hpa"
			primaryModelName   = "smoke-test-dual-primary-ms"
			secondaryModelName = "smoke-test-dual-secondary-ms"
			poolName           = "smoke-test-dual-pool"
			controllerInstance = "dual-secondary"
		)

		BeforeAll(func() {
			if cfg.ScalerBackend == scalerBackendKeda {
				Skip("Dual-controller external metrics check is specific to Prometheus Adapter backend")
			}
			if cfg.Environment != envKindEmulator {
				Skip("Dual-controller smoke scenario currently targets kind-emulator setup")
			}

			By("Creating secondary workload namespace")
			_, err := k8sClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: secondaryNamespace},
			}, metav1.CreateOptions{})
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred(), "Failed to create secondary workload namespace")
			}

			By("Installing secondary namespace-scoped controller via Kustomize")
			primaryController, err := k8sClient.AppsV1().Deployments(cfg.WVANamespace).Get(ctx, "wva-controller-manager", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Failed to read primary controller deployment image")
			Expect(primaryController.Spec.Template.Spec.Containers).NotTo(BeEmpty(), "Primary controller deployment should contain containers")
			imageRepo, imageTag := splitImage(primaryController.Spec.Template.Spec.Containers[0].Image)
			overlayPath := os.Getenv(secondaryControllerOverlayPathEnv)
			Expect(overlayPath).NotTo(BeEmpty(),
				"Missing %s; set it to the config/e2e/secondary-controller overlay directory (use an absolute path; go test cwd is the test package dir)", secondaryControllerOverlayPathEnv)
			_, statErr := os.Stat(overlayPath)
			Expect(statErr).NotTo(HaveOccurred(), "Invalid %s path: %s", secondaryControllerOverlayPathEnv, overlayPath)

			// Read the post-transform base image name from config/base/manager/kustomization.yaml.
			// The base overlay transforms "controller" → the published image name; our temp
			// overlay must match that post-transform name to override it with the local image.
			managerKustomizationPath := filepath.Join(overlayPath, "../../../../config/base/manager/kustomization.yaml")
			managerContent, managerReadErr := os.ReadFile(managerKustomizationPath)
			Expect(managerReadErr).NotTo(HaveOccurred(), "Failed to read config/base/manager/kustomization.yaml")
			var baseImageName string
			for _, line := range strings.Split(string(managerContent), "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "newName:") {
					baseImageName = strings.TrimSpace(strings.TrimPrefix(trimmed, "newName:"))
					break
				}
			}
			Expect(baseImageName).NotTo(BeEmpty(), "Failed to extract base image name from config/base/manager/kustomization.yaml")

			tmpOverlay, tmpErr := os.MkdirTemp("", "wva-secondary-overlay-*")
			Expect(tmpErr).NotTo(HaveOccurred(), "Failed to create temp overlay dir")
			// Symlink the base overlay so resources can use a relative path —
			// Kustomize rejects absolute paths in resources.
			Expect(os.Symlink(overlayPath, tmpOverlay+"/base")).To(Succeed())

			kustomizationContent := strings.Join([]string{
				"apiVersion: kustomize.config.k8s.io/v1beta1",
				"kind: Kustomization",
				"namespace: " + secondaryController,
				"resources:",
				"- ./base",
				"images:",
				"- name: " + baseImageName,
				"  newName: " + imageRepo,
				`  newTag: "` + imageTag + `"`,
				"patches:",
				"- target:",
				"    kind: Deployment",
				"    name: wva-controller-manager",
				"  patch: |",
				`    - op: add`,
				`      path: /spec/template/spec/containers/0/env/-`,
				`      value: {"name": "CONTROLLER_INSTANCE", "value": "` + controllerInstance + `"}`,
			}, "\n")
			Expect(os.WriteFile(tmpOverlay+"/kustomization.yaml", []byte(kustomizationContent), 0600)).To(Succeed())

			cmd := exec.Command("kubectl", "apply", "-k", tmpOverlay, "--server-side", "--force-conflicts")
			out, err := cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), "Secondary controller kustomize install failed: %s", string(out))

			// The secondary overlay shares the same kustomize resource names as the primary, so
			// kubectl apply overwrites the shared ClusterRoleBinding to point at the secondary
			// namespace.
			//   1. Restoring the primary's ClusterRoleBinding subject namespace.
			//   2. Creating a dedicated binding for the secondary SA.
			const crbName = "wva-manager-rolebinding"
			const crbNameSecondary = "workload-variant-autoscaler-" + crbName + "-secondary"
			restoreOut, restoreErr := exec.Command("kubectl", "patch", "clusterrolebinding", crbName,
				"--type=json",
				"-p", `[{"op":"replace","path":"/subjects/0/namespace","value":"`+cfg.WVANamespace+`"}]`,
			).CombinedOutput()
			Expect(restoreErr).NotTo(HaveOccurred(), "Failed to restore primary ClusterRoleBinding: %s", string(restoreOut))

			createOut, createErr := exec.Command("kubectl", "create", "clusterrolebinding", crbNameSecondary,
				"--clusterrole=wva-manager-role",
				"--serviceaccount="+secondaryController+":wva-controller-manager",
			).CombinedOutput()
			Expect(createErr).NotTo(HaveOccurred(), "Failed to create secondary ClusterRoleBinding: %s", string(createOut))

			// The epp-metrics-reader ClusterRoleBinding is also cluster-scoped and gets overwritten
			// by the secondary overlay — restore the primary subject and create a secondary binding.
			const eppCRBName = "wva-epp-metrics-reader-role-binding"
			const eppCRBNameSecondary = "workload-variant-autoscaler-" + eppCRBName + "-secondary"
			eppRestoreOut, eppRestoreErr := exec.Command("kubectl", "patch", "clusterrolebinding", eppCRBName,
				"--type=json",
				"-p", `[{"op":"replace","path":"/subjects/0/namespace","value":"`+cfg.WVANamespace+`"}]`,
			).CombinedOutput()
			Expect(eppRestoreErr).NotTo(HaveOccurred(), "Failed to restore primary epp-metrics ClusterRoleBinding: %s", string(eppRestoreOut))

			eppCreateOut, eppCreateErr := exec.Command("kubectl", "create", "clusterrolebinding", eppCRBNameSecondary,
				"--clusterrole=wva-epp-metrics-reader-role",
				"--serviceaccount="+secondaryController+":wva-epp-metrics-reader",
			).CombinedOutput()
			Expect(eppCreateErr).NotTo(HaveOccurred(), "Failed to create secondary epp-metrics ClusterRoleBinding: %s", string(eppCreateOut))

			// metrics-auth-rolebinding is also cluster-scoped and gets overwritten by the secondary
			// overlay — restore the primary subject and create a per-deployment secondary binding.
			const metricsAuthCRBName = "wva-metrics-auth-rolebinding"
			const metricsAuthCRBNameSecondary = "workload-variant-autoscaler-" + metricsAuthCRBName + "-secondary"
			metricsAuthRestoreOut, metricsAuthRestoreErr := exec.Command("kubectl", "patch", "clusterrolebinding", metricsAuthCRBName,
				"--type=json",
				"-p", `[{"op":"replace","path":"/subjects/0/namespace","value":"`+cfg.WVANamespace+`"}]`,
			).CombinedOutput()
			Expect(metricsAuthRestoreErr).NotTo(HaveOccurred(), "Failed to restore primary metrics-auth ClusterRoleBinding: %s", string(metricsAuthRestoreOut))

			metricsAuthCreateOut, metricsAuthCreateErr := exec.Command("kubectl", "create", "clusterrolebinding", metricsAuthCRBNameSecondary,
				"--clusterrole=wva-metrics-auth-role",
				"--serviceaccount="+secondaryController+":wva-controller-manager",
			).CombinedOutput()
			Expect(metricsAuthCreateErr).NotTo(HaveOccurred(), "Failed to create secondary metrics-auth ClusterRoleBinding: %s", string(metricsAuthCreateOut))

			DeferCleanup(func() {
				_ = exec.Command("kubectl", "delete", "clusterrolebinding", crbNameSecondary, "--ignore-not-found=true").Run()
				_ = exec.Command("kubectl", "delete", "clusterrolebinding", eppCRBNameSecondary, "--ignore-not-found=true").Run()
				_ = exec.Command("kubectl", "delete", "clusterrolebinding", metricsAuthCRBNameSecondary, "--ignore-not-found=true").Run()
				// Delete the secondary controller namespace (cascades to all namespace-scoped
				// resources). Do NOT use kubectl delete -k here — it would delete the shared
				// ClusterRoles/ClusterRoleBindings that the primary controller depends on.
				_ = exec.Command("kubectl", "delete", "namespace", secondaryController, "--ignore-not-found=true").Run()
				_ = exec.Command("kubectl", "delete", "namespace", secondaryNamespace, "--ignore-not-found=true").Run()
				_ = os.RemoveAll(tmpOverlay)
			})

			By("Waiting for secondary controller to be ready")
			Eventually(func(g Gomega) {
				pods, listErr := k8sClient.CoreV1().Pods(secondaryController).List(ctx, metav1.ListOptions{
					LabelSelector: "control-plane=controller-manager",
				})
				g.Expect(listErr).NotTo(HaveOccurred())
				g.Expect(pods.Items).NotTo(BeEmpty(), "Expected secondary controller pod")
				ready := 0
				for _, pod := range pods.Items {
					if pod.Status.Phase != corev1.PodRunning {
						continue
					}
					for _, condition := range pod.Status.Conditions {
						if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
							ready++
							break
						}
					}
				}
				g.Expect(ready).To(BeNumerically(">", 0), "Expected at least one ready secondary controller pod")
			}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Creating model services in both namespaces")
			err = fixtures.EnsureModelService(ctx, k8sClient, primaryNamespace, primaryModelName, poolName, cfg.ModelID, sharedVariantName, cfg.UseSimulator, cfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary model service")
			err = fixtures.EnsureService(ctx, k8sClient, primaryNamespace, primaryModelName, primaryModelName+"-decode", 8000)
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary service")
			err = fixtures.EnsureServiceMonitor(ctx, crClient, cfg.MonitoringNS, primaryNamespace, primaryModelName, primaryModelName+"-decode")
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary ServiceMonitor")

			err = fixtures.EnsureModelService(ctx, k8sClient, secondaryNamespace, secondaryModelName, poolName, cfg.ModelID, sharedVariantName, cfg.UseSimulator, cfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary model service")
			err = fixtures.EnsureService(ctx, k8sClient, secondaryNamespace, secondaryModelName, secondaryModelName+"-decode", 8000)
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary service")
			err = fixtures.EnsureServiceMonitor(ctx, crClient, cfg.MonitoringNS, secondaryNamespace, secondaryModelName, secondaryModelName+"-decode")
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary ServiceMonitor")

			// Create annotated HPAs (the discovery source AND scaler) with an
			// overlapping object name in both namespaces. The HPA object name is the
			// variant_name label; the WVA controller-instance label on each HPA is what
			// keeps the two namespace-scoped controllers isolated. The secondary
			// controller (CONTROLLER_INSTANCE=controllerInstance) only reconciles the
			// HPA carrying a matching label; the cluster-scoped primary has no
			// CONTROLLER_INSTANCE and reconciles the unlabeled primary HPA.
			By("Creating annotated HPAs in both namespaces with the shared variant name")
			err = fixtures.EnsureHPA(ctx, k8sClient, primaryNamespace, sharedHPABase, primaryModelName+"-decode", sharedVariantName, 1, 10,
				fixtures.WithWVAAnnotations(cfg.ModelID, "30.0"))
			Expect(err).NotTo(HaveOccurred(), "Failed to create primary HPA")
			DeferCleanup(func() {
				_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(primaryNamespace).Delete(ctx, sharedVariantName, metav1.DeleteOptions{})
			})

			err = fixtures.EnsureHPA(ctx, k8sClient, secondaryNamespace, sharedHPABase, secondaryModelName+"-decode", sharedVariantName, 1, 10,
				fixtures.WithWVAAnnotations(cfg.ModelID, "30.0"))
			Expect(err).NotTo(HaveOccurred(), "Failed to create secondary HPA")
			DeferCleanup(func() {
				_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(secondaryNamespace).Delete(ctx, sharedVariantName, metav1.DeleteOptions{})
			})

			// Stamp the controller-instance label on the secondary HPA so WVA's
			// annotation discovery attributes its synthesized variant to the secondary
			// controller (VariantAutoscalingFromHPA copies HPA labels; readyVariantAutoscalings
			// filters by wva.llmd.ai/controller-instance). The primary HPA is left
			// unlabeled so the cluster-scoped primary controller owns it.
			By("Labeling the secondary HPA with the controller-instance for isolation")
			secondaryHPA, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(secondaryNamespace).Get(ctx, sharedVariantName, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Failed to read secondary HPA for labeling")
			if secondaryHPA.Labels == nil {
				secondaryHPA.Labels = map[string]string{}
			}
			secondaryHPA.Labels[constants.ControllerInstanceLabelKey] = controllerInstance
			_, err = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(secondaryNamespace).Update(ctx, secondaryHPA, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred(), "Failed to label secondary HPA with controller-instance")
		})

		It("should expose isolated external metrics for each namespace-scoped controller", func() {
			// Both controllers now discover their variant from the annotated HPA and
			// emit wva_desired_replicas. WVA no longer writes VA .status, so isolation
			// is verified purely through the per-namespace external metrics surface:
			// each namespace must return exactly its own variant, attributed to the
			// controller that owns it.
			By("Querying external metrics for primary namespace")
			Eventually(func(g Gomega) {
				raw, err := k8sClient.RESTClient().
					Get().
					AbsPath("/apis/external.metrics.k8s.io/v1beta1/namespaces/"+primaryNamespace+"/"+constants.WVADesiredReplicas).
					Param("labelSelector", "variant_name="+sharedVariantName+",exported_namespace="+primaryNamespace).
					DoRaw(ctx)
				g.Expect(err).NotTo(HaveOccurred())
				var metricList externalMetricValueList
				g.Expect(json.Unmarshal(raw, &metricList)).To(Succeed())
				g.Expect(metricList.Items).To(HaveLen(1))
				g.Expect(metricList.Items[0].MetricLabels["exported_namespace"]).To(Equal(primaryNamespace))
			}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Querying external metrics for secondary controller namespace")
			Eventually(func(g Gomega) {
				raw, err := k8sClient.RESTClient().
					Get().
					AbsPath("/apis/external.metrics.k8s.io/v1beta1/namespaces/"+secondaryNamespace+"/"+constants.WVADesiredReplicas).
					Param("labelSelector", "variant_name="+sharedVariantName+",exported_namespace="+secondaryNamespace).
					DoRaw(ctx)
				g.Expect(err).NotTo(HaveOccurred())
				var metricList externalMetricValueList
				g.Expect(json.Unmarshal(raw, &metricList)).To(Succeed())
				g.Expect(metricList.Items).To(HaveLen(1))
				g.Expect(metricList.Items[0].MetricLabels["exported_namespace"]).To(Equal(secondaryNamespace))
				if ci, ok := metricList.Items[0].MetricLabels["controller_instance"]; ok {
					g.Expect(ci).To(Equal(controllerInstance))
				}
			}, time.Duration(cfg.EventuallyLongSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			By("Verifying both HPAs report active metric scaling")
			Eventually(func(g Gomega) {
				hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(primaryNamespace).Get(ctx, sharedVariantName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				var scalingActive *autoscalingv2.HorizontalPodAutoscalerCondition
				for i := range hpa.Status.Conditions {
					if hpa.Status.Conditions[i].Type == autoscalingv2.ScalingActive {
						scalingActive = &hpa.Status.Conditions[i]
						break
					}
				}
				if scalingActive == nil || scalingActive.Status != corev1.ConditionTrue {
					GinkgoWriter.Printf("primary HPA %s/%s conditions: %+v\n", primaryNamespace, sharedVariantName, hpa.Status.Conditions)
				}
				g.Expect(scalingActive).NotTo(BeNil(), "Primary HPA should report ScalingActive condition")
				g.Expect(scalingActive.Status).To(Equal(corev1.ConditionTrue), "Primary HPA should have external metric available")
			}, time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())

			Eventually(func(g Gomega) {
				hpa, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(secondaryNamespace).Get(ctx, sharedVariantName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				var scalingActive *autoscalingv2.HorizontalPodAutoscalerCondition
				for i := range hpa.Status.Conditions {
					if hpa.Status.Conditions[i].Type == autoscalingv2.ScalingActive {
						scalingActive = &hpa.Status.Conditions[i]
						break
					}
				}
				if scalingActive == nil || scalingActive.Status != corev1.ConditionTrue {
					GinkgoWriter.Printf("secondary HPA %s/%s conditions: %+v\n", secondaryNamespace, sharedVariantName, hpa.Status.Conditions)
				}
				g.Expect(scalingActive).NotTo(BeNil(), "Secondary HPA should report ScalingActive condition")
				g.Expect(scalingActive.Status).To(Equal(corev1.ConditionTrue), "Secondary HPA should have external metric available")
			}, time.Duration(cfg.EventuallyExtendedSec)*time.Second, time.Duration(cfg.PollIntervalSec)*time.Second).Should(Succeed())
		})
	})
})
