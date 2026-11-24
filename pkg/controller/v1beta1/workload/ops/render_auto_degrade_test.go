package ops

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func gpuPodSpec(gpus string, existingParallelism string) *corev1.PodSpec {
	env := []corev1.EnvVar{{Name: "EXISTING_VAR", Value: "keep"}}
	if existingParallelism != "" {
		env = append(env, corev1.EnvVar{Name: "PARALLELISM_SIZE", Value: existingParallelism})
	}
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "ome-container",
				Image: "vllm:latest",
				Env:   env,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse(gpus),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse(gpus),
					},
				},
			},
			{
				Name:  "metrics-sidecar",
				Image: "sidecar:latest",
			},
		},
	}
}

func envValue(c corev1.Container, name string) (string, int) {
	var val string
	var count int
	for _, e := range c.Env {
		if e.Name == name {
			val = e.Value
			count++
		}
	}
	return val, count
}

func pinnedHostname(pod *corev1.Pod) string {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpIn && len(expr.Values) == 1 {
				return expr.Values[0]
			}
		}
	}
	return ""
}

func TestRenderWithRevision_DegradedTopology_PerOrdinalOverrides(t *testing.T) {
	isvc := basicISVC()
	topo := &workload.DegradedTopology{
		TotalAccelerators: 13,
		Nodes: []workload.DegradedNodeAllocation{
			{NodeName: "tray-0", Accelerators: 4},
			{NodeName: "tray-1", Accelerators: 3},
			{NodeName: "tray-2", Accelerators: 4},
			{NodeName: "tray-3", Accelerators: 2},
		},
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentDecoder,
		Replicas:  1,
		Runners: []workload.RunnerPlan{
			{Name: workload.RunnerLeader, Size: 1},
			{Name: workload.RunnerWorker, Size: 3},
		},
	}
	inst := workload.InstancePlan{
		Index:            0,
		Incarnation:      3,
		DegradedTopology: topo,
	}
	inst.Runners = plan.RunnersFor(inst, "rev-a")
	if got := inst.TotalPods(); got != 4 {
		t.Fatalf("TotalPods = %d, want 4", got)
	}

	wantHash := DegradedTopologyHash(topo)
	if wantHash == "" {
		t.Fatal("DegradedTopologyHash returned empty string")
	}

	cases := []struct {
		runner     workload.RunnerPlan
		ordinal    int32
		wantNode   string
		wantGPUs   int64
		wantLocal  string
		wantOffset string
	}{
		{inst.Runners[0], 0, "tray-0", 4, "4", "0"},
		{inst.Runners[1], 0, "tray-1", 3, "3", "4"},
		{inst.Runners[1], 1, "tray-2", 4, "4", "7"},
		{inst.Runners[1], 2, "tray-3", 2, "2", "11"},
	}

	for _, tc := range cases {
		pod, err := testRenderWithRevision(isvc, gpuPodSpec("4", "16"), nil, plan, inst, tc.runner, tc.ordinal, "abc12345")
		if err != nil {
			t.Fatalf("render %s-%d: %v", tc.runner.Name, tc.ordinal, err)
		}
		if pod.Spec.NodeName != "" {
			t.Errorf("%s-%d: spec.nodeName = %q, want empty (must pin via NodeAffinity)", tc.runner.Name, tc.ordinal, pod.Spec.NodeName)
		}
		if got := pinnedHostname(pod); got != tc.wantNode {
			t.Errorf("%s-%d: pinned hostname = %q, want %q", tc.runner.Name, tc.ordinal, got, tc.wantNode)
		}
		main := pod.Spec.Containers[0]
		if got := main.Resources.Requests[nvidiaGPUResourceName]; got.Value() != tc.wantGPUs {
			t.Errorf("%s-%d: gpu request = %d, want %d", tc.runner.Name, tc.ordinal, got.Value(), tc.wantGPUs)
		}
		if got := main.Resources.Limits[nvidiaGPUResourceName]; got.Value() != tc.wantGPUs {
			t.Errorf("%s-%d: gpu limit = %d, want %d", tc.runner.Name, tc.ordinal, got.Value(), tc.wantGPUs)
		}
		if _, hasGPU := pod.Spec.Containers[1].Resources.Limits[nvidiaGPUResourceName]; hasGPU {
			t.Errorf("%s-%d: non-GPU sidecar unexpectedly received nvidia.com/gpu", tc.runner.Name, tc.ordinal)
		}
		for _, check := range []struct{ key, want string }{
			{"OME_POD_ACCELERATOR_COUNT", tc.wantLocal},
			{"OME_INSTANCE_ACCELERATOR_OFFSET", tc.wantOffset},
			{"OME_INSTANCE_ACCELERATOR_TOTAL", "13"},
			{"PARALLELISM_SIZE", "13"},
			{"EXISTING_VAR", "keep"},
		} {
			got, count := envValue(main, check.key)
			if count != 1 || got != check.want {
				t.Errorf("%s-%d: env %s = (%q, count=%d), want (%q, count=1)", tc.runner.Name, tc.ordinal, check.key, got, count, check.want)
			}
		}
		if got := pod.Annotations[query.AnnotationDegradedTopologyHash]; got != wantHash {
			t.Errorf("%s-%d: annotation %s = %q, want %q", tc.runner.Name, tc.ordinal, query.AnnotationDegradedTopologyHash, got, wantHash)
		}
	}
}

func TestRenderWithRevision_DegradedTopology_SingleNodeLeaderOnly(t *testing.T) {
	isvc := basicISVC()
	topo := &workload.DegradedTopology{
		TotalAccelerators: 4,
		Nodes: []workload.DegradedNodeAllocation{
			{NodeName: "tray-1", Accelerators: 4},
		},
	}
	plan := workload.ComponentPlan{
		Component: workload.ComponentDecoder,
		Replicas:  1,
		Runners: []workload.RunnerPlan{
			{Name: workload.RunnerLeader, Size: 1},
			{Name: workload.RunnerWorker, Size: 3},
		},
	}
	inst := workload.InstancePlan{
		Index:            0,
		Incarnation:      2,
		DegradedTopology: topo,
	}
	inst.Runners = plan.RunnersFor(inst, "rev-a")
	if got := plan.TotalPodsFor(inst, "rev-a"); got != 1 {
		t.Fatalf("TotalPodsFor = %d, want 1", got)
	}
	if len(inst.Runners) != 2 || inst.Runners[0].Size != 1 || inst.Runners[1].Size != 0 {
		t.Fatalf("unexpected degraded runners for 1 node: %+v", inst.Runners)
	}

	pod, err := testRenderWithRevision(isvc, gpuPodSpec("4", "16"), nil, plan, inst, inst.Runners[0], 0, "abc12345")
	if err != nil {
		t.Fatalf("render leader-0: %v", err)
	}
	if got := pinnedHostname(pod); got != "tray-1" {
		t.Errorf("pinned hostname = %q, want tray-1", got)
	}
	if val, count := envValue(pod.Spec.Containers[0], "OME_INSTANCE_ACCELERATOR_TOTAL"); count != 1 || val != "4" {
		t.Errorf("OME_INSTANCE_ACCELERATOR_TOTAL = (%q, %d), want (\"4\", 1)", val, count)
	}
	if val, count := envValue(pod.Spec.Containers[0], "PARALLELISM_SIZE"); count != 1 || val != "4" {
		t.Errorf("PARALLELISM_SIZE = (%q, %d), want (\"4\", 1)", val, count)
	}
}
