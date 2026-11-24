package inferencereplica

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

type autoDegradeHarness struct {
	t        *testing.T
	ctx      context.Context
	client   client.Client
	clock    *clocktesting.FakeClock
	recorder *record.FakeRecorder
	rec      *Reconciler
	isvc     *v1beta1.InferenceService
	prefill  types.NamespacedName
	decoder  types.NamespacedName
}

func newAutoDegradeHarness(t *testing.T, encoding irstatus.Encoding, policy *v1beta1.AutoDegradePolicy) *autoDegradeHarness {
	t.Helper()
	ctx := context.Background()
	start := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(start)

	nodes := []client.Object{
		gpuNode("prefill-0", 4, true, false),
		gpuNode("tray-0", 4, true, false),
		gpuNode("tray-1", 4, true, false),
		gpuNode("tray-2", 4, true, false),
		gpuNode("tray-3", 4, true, false),
	}

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "m3-pro",
			Namespace:  "default",
			UID:        types.UID("m3-pro-isvc-uid"),
			Generation: 1,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{runnerContainer("4", "4")},
				},
			},
			Decoder: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []corev1.Container{runnerContainer("4", "16")},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: ptr.To(3),
					PodSpec: v1beta1.PodSpec{
						Containers: []corev1.Container{runnerContainer("4", "16")},
					},
				},
				AutoDegrade: policy.DeepCopy(),
			},
		},
	}

	// Verify MergeDecoderSpec preserves AutoDegrade from ISVC over runtime defaults.
	mergedDec, err := isvcutils.MergeDecoderSpec(&v1beta1.DecoderSpec{}, isvc.Spec.Decoder)
	if err != nil {
		t.Fatalf("MergeDecoderSpec: %v", err)
	}
	if !equality.Semantic.DeepEqual(mergedDec.AutoDegrade, policy) {
		t.Fatalf("MergeDecoderSpec lost AutoDegrade: got %+v, want %+v", mergedDec.AutoDegrade, policy)
	}

	objs := append([]client.Object{isvc}, nodes...)
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}, &v1beta1.InferenceService{}, &corev1.Pod{}, &corev1.Node{}).
		Build()

	// Project both Engine (Prefill) and Decoder InferenceReplicas via irprojector.
	prefillPodSpec := &corev1.PodSpec{
		Containers: []corev1.Container{runnerContainer("4", "4")},
		Affinity:   nodeAffinityIn("prefill-0"),
	}
	leaderPodSpec := &corev1.PodSpec{
		Containers: []corev1.Container{runnerContainer("4", "16")},
		Affinity:   nodeAffinityNotIn("prefill-0"),
	}
	workerPodSpec := &corev1.PodSpec{
		Containers: []corev1.Container{runnerContainer("4", "16")},
		Affinity:   nodeAffinityNotIn("prefill-0"),
	}

	prefillIR, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
		ISVC:         isvc,
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)},
		ObjectMeta:   metav1.ObjectMeta{Labels: map[string]string{"app": "prefill"}},
		PodSpec:      prefillPodSpec,
		Client:       c,
		Reader:       c,
	})
	if err != nil {
		t.Fatalf("project prefill IR: %v", err)
	}
	decoderIR, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
		ISVC:          isvc,
		Component:     v1beta1.DecoderComponent,
		ComponentExt:  &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)},
		ObjectMeta:    metav1.ObjectMeta{Labels: map[string]string{"app": "decoder"}},
		PodSpec:       leaderPodSpec,
		WorkerPodSpec: workerPodSpec,
		WorkerSize:    3,
		MultiPod:      true,
		AutoDegrade:   mergedDec.AutoDegrade,
		Client:        c,
		Reader:        c,
	})
	if err != nil {
		t.Fatalf("project decoder IR: %v", err)
	}
	if !equality.Semantic.DeepEqual(decoderIR.Spec.AutoDegrade, policy) {
		t.Fatalf("projected decoder IR AutoDegrade = %+v, want %+v", decoderIR.Spec.AutoDegrade, policy)
	}

	// Stamp UIDs on the projected IRs (fake client leaves UID empty unless set).
	stampIRUID(t, ctx, c, prefillIR, "prefill-ir-uid")
	stampIRUID(t, ctx, c, decoderIR, "decoder-ir-uid")

	recorder := record.NewFakeRecorder(128)
	rec := &Reconciler{
		Client:                c,
		APIReader:             c,
		Log:                   logf.Log.WithName("auto-degrade-test"),
		Recorder:              recorder,
		Expectations:          workloadtypes.NewExpectations(),
		Clock:                 clk,
		InstanceStatusDecoder: irstatus.NewDecoder(64),
		InstanceStatusTarget:  encoding,
	}

	return &autoDegradeHarness{
		t:        t,
		ctx:      ctx,
		client:   c,
		clock:    clk,
		recorder: recorder,
		rec:      rec,
		isvc:     isvc,
		prefill:  types.NamespacedName{Namespace: "default", Name: prefillIR.Name},
		decoder:  types.NamespacedName{Namespace: "default", Name: decoderIR.Name},
	}
}

func stampIRUID(t *testing.T, ctx context.Context, c client.Client, ir *v1beta1.InferenceReplica, uid string) {
	t.Helper()
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(ir), got); err != nil {
		t.Fatalf("get IR %s: %v", ir.Name, err)
	}
	got.UID = types.UID(uid)
	if err := c.Update(ctx, got); err != nil {
		t.Fatalf("stamp UID on IR %s: %v", ir.Name, err)
	}
}

func gpuNode(name string, gpus int64, ready bool, unschedulable bool) *corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	qty := *resource.NewQuantity(gpus, resource.DecimalSI)
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"kubernetes.io/hostname": name},
		},
		Spec: corev1.NodeSpec{
			Unschedulable: unschedulable,
		},
		Status: corev1.NodeStatus{
			Capacity:    corev1.ResourceList{nvidiaGPUResourceName: qty.DeepCopy()},
			Allocatable: corev1.ResourceList{nvidiaGPUResourceName: qty.DeepCopy()},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: status},
			},
		},
	}
}

func runnerContainer(gpus string, parallelism string) corev1.Container {
	qty := resource.MustParse(gpus)
	return corev1.Container{
		Name:  constants.MainContainerName,
		Image: "vllm:dep-degrade",
		Env: []corev1.EnvVar{
			{Name: "PARALLELISM_SIZE", Value: parallelism},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{nvidiaGPUResourceName: qty.DeepCopy()},
			Limits:   corev1.ResourceList{nvidiaGPUResourceName: qty.DeepCopy()},
		},
	}
}

func nodeAffinityIn(node string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{node},
					}},
				}},
			},
		},
	}
}

func nodeAffinityNotIn(node string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpNotIn,
						Values:   []string{node},
					}},
				}},
			},
		},
	}
}

func (h *autoDegradeHarness) reconcile(key types.NamespacedName) ctrl.Result {
	h.t.Helper()
	// Simulate expectations watch satisfaction between passes (no live informer in unit test).
	h.rec.Expectations = workloadtypes.NewExpectations()
	res, err := h.rec.Reconcile(h.ctx, ctrl.Request{NamespacedName: key})
	if err != nil {
		h.t.Fatalf("Reconcile(%s): %v", key.Name, err)
	}
	return res
}

// scheduleAndRunPods simulates kube-scheduler + kubelet:
//   - stamps UID + CreationTimestamp on freshly created pods;
//   - accounts GPU usage per node across scheduled non-terminal pods;
//   - binds Pending pods to the first matching healthy, schedulable node with
//     enough free nvidia.com/gpu (respecting NodeAffinity In/NotIn);
//   - marks unplaceable pods Pending + PodScheduled=False (Unschedulable);
//   - transitions bound Running pods to ContainersReady=True, and mirrors
//     ome.io/serving=True onto PodReady=True (matching kubelet readiness gate).
func (h *autoDegradeHarness) scheduleAndRunPods() {
	h.t.Helper()
	now := metav1.NewTime(h.clock.Now())

	nodeList := &corev1.NodeList{}
	if err := h.client.List(h.ctx, nodeList); err != nil {
		h.t.Fatalf("list nodes: %v", err)
	}
	// Deterministic node order: prefill-0, tray-0, tray-1, tray-2, tray-3.
	nodes := make([]corev1.Node, len(nodeList.Items))
	copy(nodes, nodeList.Items)
	sortNodesByName(nodes)

	podList := &corev1.PodList{}
	if err := h.client.List(h.ctx, podList); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	pods := make([]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		pods[i] = &podList.Items[i]
	}
	// Sort pods in flat rank order so tray-0..tray-3 assignment is deterministic.
	pods = sortPodsDeterministically(pods)

	usedGPUs := make(map[string]int64)
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		if pod.Spec.NodeName != "" {
			usedGPUs[pod.Spec.NodeName] += podGPURequest(pod)
		}
	}

	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		specChanged := false
		if pod.UID == "" {
			pod.UID = uuid.NewUUID()
			specChanged = true
		}
		if pod.CreationTimestamp.IsZero() {
			pod.CreationTimestamp = now
			specChanged = true
		}
		if pod.Spec.NodeName == "" {
			reqGPUs := podGPURequest(pod)
			chosen := ""
			for i := range nodes {
				n := &nodes[i]
				if n.Spec.Unschedulable || !isNodeReady(n) {
					continue
				}
				if !podMatchesNodeAffinity(pod, n.Name) {
					continue
				}
				allocQty := n.Status.Allocatable[nvidiaGPUResourceName]
				if allocQty.Value()-usedGPUs[n.Name] >= reqGPUs {
					chosen = n.Name
					break
				}
			}
			if chosen != "" {
				pod.Spec.NodeName = chosen
				usedGPUs[chosen] += reqGPUs
				specChanged = true
			}
		}
		if specChanged {
			if err := h.client.Update(h.ctx, pod); err != nil {
				h.t.Fatalf("update pod %s: %v", pod.Name, err)
			}
		}

		// Re-fetch before status update so ResourceVersion is current.
		fresh := &corev1.Pod{}
		if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
			h.t.Fatalf("get pod %s: %v", pod.Name, err)
		}
		if fresh.Spec.NodeName == "" {
			fresh.Status.Phase = corev1.PodPending
			setAutoDegradePodCondition(&fresh.Status.Conditions, corev1.PodScheduled, corev1.ConditionFalse, corev1.PodReasonUnschedulable, now)
		} else {
			fresh.Status.Phase = corev1.PodRunning
			setAutoDegradePodCondition(&fresh.Status.Conditions, corev1.PodScheduled, corev1.ConditionTrue, "", now)
			setAutoDegradePodCondition(&fresh.Status.Conditions, corev1.ContainersReady, corev1.ConditionTrue, "", now)
			readyStatus := corev1.ConditionFalse
			if podreadiness.IsServing(fresh) {
				readyStatus = corev1.ConditionTrue
			}
			setAutoDegradePodCondition(&fresh.Status.Conditions, corev1.PodReady, readyStatus, "", now)
		}
		if err := h.client.Status().Update(h.ctx, fresh); err != nil {
			h.t.Fatalf("status update pod %s: %v", fresh.Name, err)
		}
	}
}

func (h *autoDegradeHarness) driveUntilReady(key types.NamespacedName, maxSteps int) *v1beta1.InferenceReplica {
	h.t.Helper()
	for i := 0; i < maxSteps; i++ {
		h.reconcile(key)
		h.scheduleAndRunPods()
		ir := h.getDecodedIR(key)
		if len(ir.Status.InstanceStatuses) == 1 &&
			ir.Status.InstanceStatuses[0].Phase == v1beta1.OMENativeInstanceReady &&
			ir.Status.InstanceStatuses[0].Operation == nil {
			// Run one more pass so LastPlacedNodes and published counters settle.
			h.reconcile(key)
			return h.getDecodedIR(key)
		}
	}
	ir := h.getDecodedIR(key)
	h.t.Fatalf("IR %s did not reach Ready within %d steps; status=%+v", key.Name, maxSteps, ir.Status)
	return nil
}

func (h *autoDegradeHarness) getDecodedIR(key types.NamespacedName) *v1beta1.InferenceReplica {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(h.ctx, h.rec.liveReader(), key, ir); err != nil {
		h.t.Fatalf("GetDecoded(%s): %v", key.Name, err)
	}
	return ir
}

func (h *autoDegradeHarness) listComponentPods(component v1beta1.ComponentType) []*corev1.Pod {
	h.t.Helper()
	pods, err := query.ListOMENativePodsByName(h.ctx, h.client, "default", "m3-pro", workloadtypes.ComponentType(component), true)
	if err != nil {
		h.t.Fatalf("list pods for %s: %v", component, err)
	}
	return sortPodsDeterministically(pods)
}

func (h *autoDegradeHarness) failPodOnNode(component v1beta1.ComponentType, nodeName string) {
	h.t.Helper()
	pods := h.listComponentPods(component)
	for _, pod := range pods {
		if pod.Spec.NodeName == nodeName && pod.Status.Phase != corev1.PodFailed {
			pod.Status.Phase = corev1.PodFailed
			setAutoDegradePodCondition(&pod.Status.Conditions, corev1.ContainersReady, corev1.ConditionFalse, "Error", metav1.NewTime(h.clock.Now()))
			setAutoDegradePodCondition(&pod.Status.Conditions, corev1.PodReady, corev1.ConditionFalse, "Error", metav1.NewTime(h.clock.Now()))
			if err := h.client.Status().Update(h.ctx, pod); err != nil {
				h.t.Fatalf("fail pod %s: %v", pod.Name, err)
			}
			return
		}
	}
	h.t.Fatalf("no live %s pod found on node %s", component, nodeName)
}

func (h *autoDegradeHarness) setNodeAllocatableGPUs(nodeName string, gpus int64) {
	h.t.Helper()
	node := &corev1.Node{}
	if err := h.client.Get(h.ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		h.t.Fatalf("get node %s: %v", nodeName, err)
	}
	node.Status.Allocatable[nvidiaGPUResourceName] = *resource.NewQuantity(gpus, resource.DecimalSI)
	if err := h.client.Status().Update(h.ctx, node); err != nil {
		h.t.Fatalf("update node %s allocatable: %v", nodeName, err)
	}
}

func (h *autoDegradeHarness) cordonNode(nodeName string) {
	h.t.Helper()
	node := &corev1.Node{}
	if err := h.client.Get(h.ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		h.t.Fatalf("get node %s: %v", nodeName, err)
	}
	node.Spec.Unschedulable = true
	if err := h.client.Update(h.ctx, node); err != nil {
		h.t.Fatalf("cordon node %s: %v", nodeName, err)
	}
}

func (h *autoDegradeHarness) drainEvents() []string {
	var out []string
	for {
		select {
		case ev := <-h.recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func sortNodesByName(nodes []corev1.Node) {
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if nodes[j].Name < nodes[i].Name {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}
}

func sortPodsDeterministically(pods []*corev1.Pod) []*corev1.Pod {
	out := append([]*corev1.Pod(nil), pods...)
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if podLess(out[j], out[i]) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func podLess(a, b *corev1.Pod) bool {
	ra, rb := runnerSortOrder(a.Labels[query.LabelRunner]), runnerSortOrder(b.Labels[query.LabelRunner])
	if ra != rb {
		return ra < rb
	}
	oa, ob := podOrdinalOf(a), podOrdinalOf(b)
	if oa != ob {
		return oa < ob
	}
	return a.Name < b.Name
}

func podGPURequest(pod *corev1.Pod) int64 {
	var total int64
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if q, ok := c.Resources.Requests[nvidiaGPUResourceName]; ok {
			total += q.Value()
		} else if q, ok := c.Resources.Limits[nvidiaGPUResourceName]; ok {
			total += q.Value()
		}
	}
	return total
}

func podMatchesNodeAffinity(pod *corev1.Pod, nodeName string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return true
	}
	for _, term := range terms {
		matched := true
		for _, expr := range term.MatchExpressions {
			if expr.Key != "kubernetes.io/hostname" {
				continue
			}
			inVals := false
			for _, v := range expr.Values {
				if v == nodeName {
					inVals = true
					break
				}
			}
			switch expr.Operator {
			case corev1.NodeSelectorOpIn:
				if !inVals {
					matched = false
				}
			case corev1.NodeSelectorOpNotIn:
				if inVals {
					matched = false
				}
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func setAutoDegradePodCondition(conds *[]corev1.PodCondition, condType corev1.PodConditionType, status corev1.ConditionStatus, reason string, now metav1.Time) {
	for i := range *conds {
		if (*conds)[i].Type == condType {
			if (*conds)[i].Status != status || (*conds)[i].Reason != reason {
				(*conds)[i].Status = status
				(*conds)[i].Reason = reason
				(*conds)[i].LastTransitionTime = now
			}
			return
		}
	}
	*conds = append(*conds, corev1.PodCondition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: now,
	})
}

func assertDegradedPods(t *testing.T, pods []*corev1.Pod, wantTopo *v1beta1.DegradedTopology) {
	t.Helper()
	if len(pods) != len(wantTopo.Nodes) {
		t.Fatalf("pod count = %d, want %d", len(pods), len(wantTopo.Nodes))
	}
	wantHash := workloadops.DegradedTopologyHash(v1beta1convert.DegradedTopologyToWorkload(wantTopo))
	var offset int32
	for rank, alloc := range wantTopo.Nodes {
		pod := pods[rank]
		if pod.Spec.NodeName != alloc.NodeName {
			t.Errorf("rank %d pod %s scheduled on %q, want %q", rank, pod.Name, pod.Spec.NodeName, alloc.NodeName)
		}
		if !hasPinnedHostname(pod, alloc.NodeName) {
			t.Errorf("rank %d pod %s missing required NodeAffinity hostname In [%s]", rank, pod.Name, alloc.NodeName)
		}
		if got := podGPURequest(pod); got != int64(alloc.Accelerators) {
			t.Errorf("rank %d pod %s gpu request = %d, want %d", rank, pod.Name, got, alloc.Accelerators)
		}
		lim := pod.Spec.Containers[0].Resources.Limits[nvidiaGPUResourceName]
		if lim.Value() != int64(alloc.Accelerators) {
			t.Errorf("rank %d pod %s gpu limit = %d, want %d", rank, pod.Name, lim.Value(), alloc.Accelerators)
		}
		wantRunner := workloadtypes.RunnerWorker
		if rank == 0 {
			wantRunner = workloadtypes.RunnerLeader
		}
		if pod.Labels[query.LabelRunner] != wantRunner {
			t.Errorf("rank %d pod %s runner = %q, want %q", rank, pod.Name, pod.Labels[query.LabelRunner], wantRunner)
		}
		for _, kv := range [][2]string{
			{"OME_POD_ACCELERATOR_COUNT", fmt.Sprintf("%d", alloc.Accelerators)},
			{"OME_INSTANCE_ACCELERATOR_OFFSET", fmt.Sprintf("%d", offset)},
			{"OME_INSTANCE_ACCELERATOR_TOTAL", fmt.Sprintf("%d", wantTopo.TotalAccelerators)},
			{"PARALLELISM_SIZE", fmt.Sprintf("%d", wantTopo.TotalAccelerators)},
		} {
			got := containerEnv(pod.Spec.Containers[0], kv[0])
			if got != kv[1] {
				t.Errorf("rank %d pod %s env %s = %q, want %q", rank, pod.Name, kv[0], got, kv[1])
			}
		}
		if pod.Annotations[query.AnnotationDegradedTopologyHash] != wantHash {
			t.Errorf("rank %d pod %s annotation %s = %q, want %q", rank, pod.Name, query.AnnotationDegradedTopologyHash, pod.Annotations[query.AnnotationDegradedTopologyHash], wantHash)
		}
		offset += alloc.Accelerators
	}
}

func hasPinnedHostname(pod *corev1.Pod, node string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpIn &&
				len(expr.Values) == 1 && expr.Values[0] == node {
				return true
			}
		}
	}
	return false
}

func containerEnv(c corev1.Container, name string) string {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func TestAutoDegrade_DEP16_To_DEP15_To_DEP14_PrefillUntouched(t *testing.T) {
	for _, enc := range []irstatus.Encoding{irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2} {
		t.Run(string(enc), func(t *testing.T) {
			policy := &v1beta1.AutoDegradePolicy{
				Enabled:               true,
				MinTotalAccelerators:  12,
				TriggerTimeoutSeconds: 30,
				CooldownSeconds:       60,
			}
			h := newAutoDegradeHarness(t, enc, policy)

			// Baseline DEP16 serving: Prefill (1 pod on prefill-0) + Decode (4 pods on tray-0..3).
			h.driveUntilReady(h.prefill, 10)
			decIR := h.driveUntilReady(h.decoder, 15)

			prefillPodsBefore := h.listComponentPods(v1beta1.EngineComponent)
			if len(prefillPodsBefore) != 1 {
				t.Fatalf("prefill pod count = %d, want 1", len(prefillPodsBefore))
			}
			prefillUID := prefillPodsBefore[0].UID
			isvcSpecBefore := h.isvc.Spec.DeepCopy()

			wantPlaced := []string{"tray-0", "tray-1", "tray-2", "tray-3"}
			if got := decIR.Status.InstanceStatuses[0].LastPlacedNodes; !reflect.DeepEqual(got, wantPlaced) {
				t.Fatalf("baseline LastPlacedNodes = %v, want %v", got, wantPlaced)
			}
			if decIR.Status.InstanceStatuses[0].DegradedTopology != nil {
				t.Fatalf("baseline DegradedTopology = %+v, want nil", decIR.Status.InstanceStatuses[0].DegradedTopology)
			}
			h.drainEvents()

			// Fault #1: kill pod on tray-3 + drop tray-3 allocatable 4 -> 3.
			h.setNodeAllocatableGPUs("tray-3", 3)
			h.failPodOnNode(v1beta1.DecoderComponent, "tray-3")

			// Drive Restart through drain -> delete -> full DEP16 recreate attempt.
			// worker-2 stays Pending/Unschedulable because tray-3 only has 3 GPUs.
			for i := 0; i < 6; i++ {
				h.reconcile(h.decoder)
				h.scheduleAndRunPods()
			}
			// Before TriggerTimeoutSeconds (30s), must NOT degrade yet and MUST request requeue.
			h.clock.Step(10 * time.Second)
			res := h.reconcile(h.decoder)
			if res.RequeueAfter <= 0 || res.RequeueAfter > 20*time.Second {
				t.Fatalf("before timeout RequeueAfter = %v, want (0, 20s]", res.RequeueAfter)
			}
			if got := h.getDecodedIR(h.decoder).Status.InstanceStatuses[0].DegradedTopology; got != nil {
				t.Fatalf("degraded prematurely before TriggerTimeoutSeconds: %+v", got)
			}

			// Advance past TriggerTimeoutSeconds -> automatic DEP16 -> DEP15 [4,4,4,3].
			h.clock.Step(21 * time.Second)
			decIR = h.driveUntilReady(h.decoder, 15)

			gotTopo15 := decIR.Status.InstanceStatuses[0].DegradedTopology
			if gotTopo15 == nil {
				t.Fatal("expected DegradedTopology for DEP15, got nil")
			}
			wantNodes15 := []v1beta1.DegradedNodeAllocation{
				{NodeName: "tray-0", Accelerators: 4},
				{NodeName: "tray-1", Accelerators: 4},
				{NodeName: "tray-2", Accelerators: 4},
				{NodeName: "tray-3", Accelerators: 3},
			}
			if gotTopo15.TotalAccelerators != 15 || !reflect.DeepEqual(gotTopo15.Nodes, wantNodes15) {
				t.Fatalf("DEP15 DegradedTopology = %+v, want total=15 nodes=%+v", gotTopo15, wantNodes15)
			}
			assertDegradedPods(t, h.listComponentPods(v1beta1.DecoderComponent), gotTopo15)

			events := h.drainEvents()
			foundAutoDegraded := false
			for _, ev := range events {
				if strings.Contains(ev, string(workloadtypes.EventReasonAutoDegraded)) && strings.Contains(ev, "16 to 15") {
					foundAutoDegraded = true
				}
			}
			if !foundAutoDegraded {
				t.Errorf("expected AutoDegraded event for 16->15, got events: %v", events)
			}

			// Verify steady-state reconcile does NOT loop or kill degraded pods.
			inc15 := decIR.Status.InstanceStatuses[0].Incarnation
			h.reconcile(h.decoder)
			h.reconcile(h.decoder)
			decIRAfterIdle := h.getDecodedIR(h.decoder)
			if decIRAfterIdle.Status.InstanceStatuses[0].Incarnation != inc15 ||
				decIRAfterIdle.Status.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceReady {
				t.Fatalf("steady-state reconcile drifted degraded DEP15: inc %d->%d phase=%s",
					inc15, decIRAfterIdle.Status.InstanceStatuses[0].Incarnation, decIRAfterIdle.Status.InstanceStatuses[0].Phase)
			}

			// Verify InferenceService.Spec untouched & Prefill 0 restarts.
			liveISVC := &v1beta1.InferenceService{}
			if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(h.isvc), liveISVC); err != nil {
				t.Fatalf("get ISVC: %v", err)
			}
			if !equality.Semantic.DeepEqual(liveISVC.Spec, *isvcSpecBefore) {
				t.Fatalf("InferenceService.Spec was mutated during auto-degrade")
			}
			h.reconcile(h.prefill)
			prefillPodsAfter15 := h.listComponentPods(v1beta1.EngineComponent)
			if len(prefillPodsAfter15) != 1 || prefillPodsAfter15[0].UID != prefillUID {
				t.Fatalf("Prefill pod restarted during DEP15 degrade: before=%s after=%+v", prefillUID, prefillPodsAfter15)
			}

			// Fault #2: advance past cooldown (60s), kill pod on tray-2 + drop tray-2 4 -> 3.
			h.clock.Step(65 * time.Second)
			h.setNodeAllocatableGPUs("tray-2", 3)
			h.failPodOnNode(v1beta1.DecoderComponent, "tray-2")

			for i := 0; i < 6; i++ {
				h.reconcile(h.decoder)
				h.scheduleAndRunPods()
			}
			h.clock.Step(31 * time.Second)
			decIR = h.driveUntilReady(h.decoder, 15)

			gotTopo14 := decIR.Status.InstanceStatuses[0].DegradedTopology
			if gotTopo14 == nil {
				t.Fatal("expected DegradedTopology for DEP14, got nil")
			}
			wantNodes14 := []v1beta1.DegradedNodeAllocation{
				{NodeName: "tray-0", Accelerators: 4},
				{NodeName: "tray-1", Accelerators: 4},
				{NodeName: "tray-2", Accelerators: 3},
				{NodeName: "tray-3", Accelerators: 3},
			}
			if gotTopo14.TotalAccelerators != 14 || !reflect.DeepEqual(gotTopo14.Nodes, wantNodes14) {
				t.Fatalf("DEP14 DegradedTopology = %+v, want total=14 nodes=%+v", gotTopo14, wantNodes14)
			}
			assertDegradedPods(t, h.listComponentPods(v1beta1.DecoderComponent), gotTopo14)

			// Verify steady-state DEP14 stability, ISVC spec untouched, Prefill 0 restarts.
			inc14 := decIR.Status.InstanceStatuses[0].Incarnation
			h.reconcile(h.decoder)
			if got := h.getDecodedIR(h.decoder).Status.InstanceStatuses[0].Incarnation; got != inc14 {
				t.Fatalf("steady-state DEP14 drifted incarnation %d -> %d", inc14, got)
			}
			prefillPodsAfter14 := h.listComponentPods(v1beta1.EngineComponent)
			if len(prefillPodsAfter14) != 1 || prefillPodsAfter14[0].UID != prefillUID {
				t.Fatalf("Prefill pod restarted during DEP14 degrade")
			}
		})
	}
}

func TestAutoDegrade_LeaderNodeCordoned_PromotesNextNode(t *testing.T) {
	policy := &v1beta1.AutoDegradePolicy{
		Enabled:               true,
		MinTotalAccelerators:  12,
		TriggerTimeoutSeconds: 30,
		CooldownSeconds:       60,
	}
	h := newAutoDegradeHarness(t, irstatus.EncodingDenseV1, policy)
	h.driveUntilReady(h.decoder, 15)

	// Cordon leader node tray-0 and kill its pod.
	h.cordonNode("tray-0")
	h.failPodOnNode(v1beta1.DecoderComponent, "tray-0")

	for i := 0; i < 6; i++ {
		h.reconcile(h.decoder)
		h.scheduleAndRunPods()
	}
	h.clock.Step(31 * time.Second)
	decIR := h.driveUntilReady(h.decoder, 15)

	gotTopo := decIR.Status.InstanceStatuses[0].DegradedTopology
	if gotTopo == nil {
		t.Fatal("expected DegradedTopology after leader cordon, got nil")
	}
	wantNodes := []v1beta1.DegradedNodeAllocation{
		{NodeName: "tray-1", Accelerators: 4},
		{NodeName: "tray-2", Accelerators: 4},
		{NodeName: "tray-3", Accelerators: 4},
	}
	if gotTopo.TotalAccelerators != 12 || !reflect.DeepEqual(gotTopo.Nodes, wantNodes) {
		t.Fatalf("cordoned-leader DegradedTopology = %+v, want total=12 nodes=%+v", gotTopo, wantNodes)
	}
	pods := h.listComponentPods(v1beta1.DecoderComponent)
	assertDegradedPods(t, pods, gotTopo)
	if pods[0].Labels[query.LabelRunner] != workloadtypes.RunnerLeader || pods[0].Spec.NodeName != "tray-1" {
		t.Fatalf("expected tray-1 promoted to leader, got runner=%s node=%s", pods[0].Labels[query.LabelRunner], pods[0].Spec.NodeName)
	}
}

func TestAutoDegrade_FloorGuard_RefusesBelowMinTotal(t *testing.T) {
	policy := &v1beta1.AutoDegradePolicy{
		Enabled:               true,
		MinTotalAccelerators:  12,
		TriggerTimeoutSeconds: 30,
		CooldownSeconds:       60,
	}
	h := newAutoDegradeHarness(t, irstatus.EncodingDenseV1, policy)
	h.driveUntilReady(h.decoder, 15)
	h.drainEvents()

	// Drop tray-2 and tray-3 to 1 GPU each -> total 4+4+1+1 = 10 < 12.
	h.setNodeAllocatableGPUs("tray-2", 1)
	h.setNodeAllocatableGPUs("tray-3", 1)
	h.failPodOnNode(v1beta1.DecoderComponent, "tray-3")

	for i := 0; i < 6; i++ {
		h.reconcile(h.decoder)
		h.scheduleAndRunPods()
	}
	h.clock.Step(35 * time.Second)
	h.reconcile(h.decoder)

	decIR := h.getDecodedIR(h.decoder)
	if got := decIR.Status.InstanceStatuses[0].DegradedTopology; got != nil {
		t.Fatalf("expected floor guard to refuse degrade below 12, got %+v", got)
	}
	events := h.drainEvents()
	foundRefused := false
	for _, ev := range events {
		if strings.Contains(ev, string(workloadtypes.EventReasonAutoDegradeRefused)) {
			foundRefused = true
		}
	}
	if !foundRefused {
		t.Errorf("expected AutoDegradeRefused event, got: %v", events)
	}
}

func TestAutoDegrade_CooldownGuard_DelaysSecondDegrade(t *testing.T) {
	policy := &v1beta1.AutoDegradePolicy{
		Enabled:               true,
		MinTotalAccelerators:  12,
		TriggerTimeoutSeconds: 30,
		CooldownSeconds:       600,
	}
	h := newAutoDegradeHarness(t, irstatus.EncodingDenseV1, policy)
	h.driveUntilReady(h.decoder, 15)

	// First degrade: DEP16 -> DEP15.
	h.setNodeAllocatableGPUs("tray-3", 3)
	h.failPodOnNode(v1beta1.DecoderComponent, "tray-3")
	for i := 0; i < 6; i++ {
		h.reconcile(h.decoder)
		h.scheduleAndRunPods()
	}
	h.clock.Step(31 * time.Second)
	decIR := h.driveUntilReady(h.decoder, 15)
	if decIR.Status.InstanceStatuses[0].DegradedTopology.TotalAccelerators != 15 {
		t.Fatalf("expected DEP15 after first fault, got %+v", decIR.Status.InstanceStatuses[0].DegradedTopology)
	}

	// Second fault inside 600s cooldown window (at +100s).
	h.clock.Step(100 * time.Second)
	h.setNodeAllocatableGPUs("tray-2", 3)
	h.failPodOnNode(v1beta1.DecoderComponent, "tray-2")
	for i := 0; i < 6; i++ {
		h.reconcile(h.decoder)
		h.scheduleAndRunPods()
	}
	h.clock.Step(35 * time.Second)
	res := h.reconcile(h.decoder)

	// Cooldown must prevent second degrade and requeue for remaining cooldown (~465s).
	decIR = h.getDecodedIR(h.decoder)
	if decIR.Status.InstanceStatuses[0].DegradedTopology.TotalAccelerators != 15 {
		t.Fatalf("cooldown failed to block second degrade: got %+v", decIR.Status.InstanceStatuses[0].DegradedTopology)
	}
	if res.RequeueAfter <= 400*time.Second || res.RequeueAfter > 500*time.Second {
		t.Fatalf("expected RequeueAfter near remaining cooldown (~465s), got %v", res.RequeueAfter)
	}

	// Once cooldown elapses, automatic degrade to DEP14 succeeds.
	h.clock.Step(470 * time.Second)
	decIR = h.driveUntilReady(h.decoder, 15)
	if got := decIR.Status.InstanceStatuses[0].DegradedTopology; got == nil || got.TotalAccelerators != 14 {
		t.Fatalf("expected DEP14 after cooldown elapsed, got %+v", got)
	}
}
