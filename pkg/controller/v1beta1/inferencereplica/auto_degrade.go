package inferencereplica

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	defaultAutoDegradeTriggerTimeout = 60 * time.Second
	defaultAutoDegradeCooldown       = 600 * time.Second
	nvidiaGPUResourceName            = corev1.ResourceName("nvidia.com/gpu")
)

// reconcileAutoDegrade tracks each Instance's LastPlacedNodes and, when
// spec.autoDegrade.enabled is true and an Instance has Pending/Unschedulable
// pods for at least triggerTimeoutSeconds, inspects the live K8s Node objects
// for that Instance's tracked nodes and commits an in-place DegradedTopology
// in InferenceReplica.Status (never mutating Spec), triggering a fresh
// incarnation rebuild.
func (p *replicaPass) reconcileAutoDegrade(ctx context.Context) (bool, error) {
	policy := p.ir.Spec.AutoDegrade
	if policy == nil || !policy.Enabled || p.input.DesiredSpec.Paused {
		return true, nil
	}

	pods, err := p.cachedPods.Pods(ctx, componentPodLister(p.reconciler.Client, p.ir))
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: auto-degrade list pods: %w", err)
	}
	pods, _, err = query.SplitPodsOnMissingNodes(ctx, p.nodes, pods)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: auto-degrade filter missing-node pods: %w", err)
	}
	byInstance := p.cachedPods.ByInstance(pods, query.BucketPodsByInstanceIdx)

	now := p.reconciler.now()
	triggerTimeout := effectiveAutoDegradeTriggerTimeout(policy)
	cooldown := effectiveAutoDegradeCooldown(policy)
	nodeReader := client.Reader(p.reconciler.APIReader)
	if nodeReader == nil {
		nodeReader = p.reconciler.Client
	}

	degradedAny := false
	for _, inst := range p.plan.Instances {
		row := p.input.ObservedState.Instance(inst.Index)
		if row == nil {
			continue
		}

		activePods := orderedActivePodsForInstance(byInstance[inst.Index], row.Incarnation)
		scheduledNodes := uniqueScheduledNodes(activePods)

		// Track LastPlacedNodes whenever the Instance's pods are scheduled so
		// RecreateInstanceOnPodRestart never forgets the original trays after
		// old pods are deleted.
		lastPlacedNodes := append([]string(nil), row.LastPlacedNodes...)
		shouldUpdatePlaced := false
		if row.DegradedTopology == nil && int32(len(scheduledNodes)) >= inst.TotalPods() && len(scheduledNodes) > 0 {
			if !slices.Equal(lastPlacedNodes, scheduledNodes) {
				lastPlacedNodes = scheduledNodes
				shouldUpdatePlaced = true
			}
		} else if len(lastPlacedNodes) == 0 && len(scheduledNodes) > 0 {
			lastPlacedNodes = scheduledNodes
			shouldUpdatePlaced = true
		}
		if shouldUpdatePlaced {
			recorded := append([]string(nil), lastPlacedNodes...)
			if err := p.input.MutateInstance(ctx, inst.Index, func(s *workloadtypes.InstanceStatus) bool {
				if slices.Equal(s.LastPlacedNodes, recorded) {
					return false
				}
				s.LastPlacedNodes = append([]string(nil), recorded...)
				return true
			}); err != nil {
				return false, fmt.Errorf("InferenceReplica reconciler: auto-degrade persist lastPlacedNodes (instance=%d): %w", inst.Index, err)
			}
			row.LastPlacedNodes = recorded
		}

		if row.Phase == workloadtypes.InstancePhaseDeleting || row.Phase == workloadtypes.InstancePhaseMigrating {
			continue
		}

		pendingSince, hasPending := earliestPendingPodTime(activePods, now)
		if !hasPending {
			continue
		}

		elapsed := now.Sub(pendingSince)
		if elapsed < triggerTimeout {
			p.noteAutoDegradeRequeue(triggerTimeout-elapsed, int32(len(activePods)) >= inst.TotalPods())
			continue
		}

		if row.DegradedTopology != nil && row.DegradedTopology.LastTransitionTime != nil {
			sinceLast := now.Sub(row.DegradedTopology.LastTransitionTime.Time)
			if sinceLast < cooldown {
				p.noteAutoDegradeRequeue(cooldown-sinceLast, int32(len(activePods)) >= inst.TotalPods())
				continue
			}
		}

		candidateNodes := buildAutoDegradeCandidateNodes(row.DegradedTopology, lastPlacedNodes, activePods)
		if len(candidateNodes) == 0 {
			continue
		}

		leaderGPUs := templateGPUsForPodSpec(p.input.DesiredSpec.PodSpec)
		workerGPUs := templateGPUsForPodSpec(p.input.DesiredSpec.WorkerPodSpec)
		if workerGPUs == 0 {
			workerGPUs = leaderGPUs
		}
		if leaderGPUs <= 0 {
			continue
		}

		currentTargetTotal := fullTemplateTotalGPUs(p.input.DesiredSpec, leaderGPUs, workerGPUs)
		if row.DegradedTopology != nil && row.DegradedTopology.TotalAccelerators > 0 {
			currentTargetTotal = row.DegradedTopology.TotalAccelerators
		}

		allocs, total, err := buildDegradedNodeAllocations(ctx, nodeReader, candidateNodes, leaderGPUs, workerGPUs)
		if err != nil {
			return false, fmt.Errorf("InferenceReplica reconciler: auto-degrade inspect nodes (instance=%d): %w", inst.Index, err)
		}
		if len(allocs) == 0 || total >= currentTargetTotal {
			continue
		}
		if policy.MinTotalAccelerators > 0 && total < policy.MinTotalAccelerators {
			recorded, err := workloadstatus.Announce(ctx, p.input, inst.Index, workloadtypes.EventReasonAutoDegradeRefused)
			if err != nil {
				return false, fmt.Errorf("InferenceReplica reconciler: auto-degrade record refusal (instance=%d): %w", inst.Index, err)
			}
			if recorded {
				workloadtypes.RecordWarning(p.deps.Recorder, workloadtypes.EventTarget(p.input), workloadtypes.EventReasonAutoDegradeRefused,
					"OMENative %s auto-degrade refused: surviving %d accelerators across %d node(s) is below minTotalAccelerators=%d",
					workloadtypes.InstanceKey(p.input.Key.Component, inst.Index), total, len(allocs), policy.MinTotalAccelerators)
			}
			continue
		}

		nowMeta := metav1.NewTime(now.Truncate(time.Second))
		newTopo := &workloadtypes.DegradedTopology{
			TotalAccelerators:  total,
			LastTransitionTime: &nowMeta,
			Nodes:              allocs,
		}
		reason := fmt.Sprintf("auto-degrade %d->%d accelerators (%s)", currentTargetTotal, total, formatDegradedNodes(allocs))
		rollTargetName := ""
		if p.rollTarget != nil {
			rollTargetName = p.rollTarget.Name
		}

		newIncarnation, committed, err := workloadstatus.CommitDegradedTopology(
			ctx,
			p.input,
			inst.Index,
			newTopo,
			cooldown,
			rollTargetName,
			reason,
			p.instanceReadyTimeout,
		)
		if err != nil {
			return false, fmt.Errorf("InferenceReplica reconciler: auto-degrade commit topology (instance=%d): %w", inst.Index, err)
		}
		if committed {
			p.log.Info("Auto-degrading Instance in-place",
				"instance", inst.Index,
				"fromAccelerators", currentTargetTotal,
				"toAccelerators", total,
				"incarnation", newIncarnation,
				"nodes", formatDegradedNodes(allocs))
			workloadtypes.RecordWarning(p.deps.Recorder, workloadtypes.EventTarget(p.input), workloadtypes.EventReasonAutoDegraded,
				"OMENative %s automatically degraded in-place from %d to %d accelerators (incarnation=%d): %s",
				workloadtypes.InstanceKey(p.input.Key.Component, inst.Index), currentTargetTotal, total, newIncarnation, formatDegradedNodes(allocs))
			degradedAny = true
		}
	}

	if degradedAny {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	return true, nil
}

func (p *replicaPass) noteAutoDegradeRequeue(d time.Duration, allPodsExist bool) {
	if d <= 0 {
		return
	}
	if p.autoDegradeRequeue == 0 || d < p.autoDegradeRequeue {
		p.autoDegradeRequeue = d
		p.autoDegradeWaitingOnPlacement = allPodsExist
	} else if !allPodsExist {
		p.autoDegradeWaitingOnPlacement = false
	}
}

func effectiveAutoDegradeTriggerTimeout(policy *v1beta1.AutoDegradePolicy) time.Duration {
	if policy != nil && policy.TriggerTimeoutSeconds > 0 {
		return time.Duration(policy.TriggerTimeoutSeconds) * time.Second
	}
	return defaultAutoDegradeTriggerTimeout
}

func effectiveAutoDegradeCooldown(policy *v1beta1.AutoDegradePolicy) time.Duration {
	if policy != nil && policy.CooldownSeconds > 0 {
		return time.Duration(policy.CooldownSeconds) * time.Second
	}
	return defaultAutoDegradeCooldown
}

func orderedActivePodsForInstance(pods []*corev1.Pod, incarnation int64) []*corev1.Pod {
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		if incarnation > 0 {
			raw, ok := pod.Labels[query.LabelInstanceIncarnation]
			if !ok {
				continue
			}
			podInc, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || podInc != incarnation {
				continue
			}
		}
		out = append(out, pod)
	}
	slices.SortFunc(out, func(a, b *corev1.Pod) int {
		ra, rb := runnerSortOrder(a.Labels[query.LabelRunner]), runnerSortOrder(b.Labels[query.LabelRunner])
		if ra != rb {
			return ra - rb
		}
		oa, ob := podOrdinalOf(a), podOrdinalOf(b)
		if oa != ob {
			return oa - ob
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func runnerSortOrder(runner string) int {
	switch runner {
	case workloadtypes.RunnerLeader, workloadtypes.RunnerDefault:
		return 0
	case workloadtypes.RunnerWorker:
		return 1
	default:
		return 2
	}
}

func podOrdinalOf(pod *corev1.Pod) int {
	if pod == nil {
		return 0
	}
	n, err := strconv.Atoi(pod.Labels[query.LabelPodOrdinal])
	if err != nil {
		return 0
	}
	return n
}

func uniqueScheduledNodes(pods []*corev1.Pod) []string {
	seen := make(map[string]struct{}, len(pods))
	out := make([]string, 0, len(pods))
	for _, pod := range pods {
		if pod == nil || pod.Spec.NodeName == "" {
			continue
		}
		if _, ok := seen[pod.Spec.NodeName]; ok {
			continue
		}
		seen[pod.Spec.NodeName] = struct{}{}
		out = append(out, pod.Spec.NodeName)
	}
	return out
}

func earliestPendingPodTime(pods []*corev1.Pod, now time.Time) (time.Time, bool) {
	var earliest time.Time
	found := false
	for _, pod := range pods {
		if pod == nil || pod.Spec.NodeName != "" || (pod.Status.Phase != corev1.PodPending && pod.Status.Phase != "") {
			continue
		}
		if workloadtypes.PodAdmissionGated(pod) {
			continue
		}
		var since time.Time
		for i := range pod.Status.Conditions {
			cond := &pod.Status.Conditions[i]
			if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse && !cond.LastTransitionTime.IsZero() {
				since = cond.LastTransitionTime.Time
				break
			}
		}
		if since.IsZero() && !pod.CreationTimestamp.IsZero() {
			since = pod.CreationTimestamp.Time
		}
		if since.IsZero() {
			since = now
		}
		if !found || since.Before(earliest) {
			earliest = since
			found = true
		}
	}
	return earliest, found
}

func buildAutoDegradeCandidateNodes(degraded *workloadtypes.DegradedTopology, lastPlaced []string, activePods []*corev1.Pod) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(lastPlaced)+len(activePods))
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	if degraded != nil && len(degraded.Nodes) > 0 {
		for _, n := range degraded.Nodes {
			add(n.NodeName)
		}
	} else {
		for _, n := range lastPlaced {
			add(n)
		}
	}
	for _, pod := range activePods {
		if pod != nil {
			add(pod.Spec.NodeName)
		}
	}
	return out
}

func templateGPUsForPodSpec(spec *corev1.PodSpec) int32 {
	if spec == nil {
		return 0
	}
	var total int32
	for i := range spec.Containers {
		c := &spec.Containers[i]
		var gpus int64
		if q, ok := c.Resources.Limits[nvidiaGPUResourceName]; ok && q.Value() > gpus {
			gpus = q.Value()
		}
		if q, ok := c.Resources.Requests[nvidiaGPUResourceName]; ok && q.Value() > gpus {
			gpus = q.Value()
		}
		total += int32(gpus)
	}
	return total
}

func fullTemplateTotalGPUs(desired workloadtypes.WorkloadDesiredSpec, leaderGPUs, workerGPUs int32) int32 {
	if !desired.MultiPod {
		return leaderGPUs
	}
	return leaderGPUs + workloadtypes.WorkerSizeOf(desired.Runners)*workerGPUs
}

func isNodeReady(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == corev1.NodeReady {
			return node.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

func buildDegradedNodeAllocations(ctx context.Context, reader client.Reader, candidateNodes []string, leaderGPUs, workerGPUs int32) ([]workloadtypes.DegradedNodeAllocation, int32, error) {
	allocs := make([]workloadtypes.DegradedNodeAllocation, 0, len(candidateNodes))
	var total int32
	for _, nodeName := range candidateNodes {
		node := &corev1.Node{}
		if err := reader.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, 0, err
		}
		if node.Spec.Unschedulable || !isNodeReady(node) {
			continue
		}
		gpuQty := node.Status.Allocatable[nvidiaGPUResourceName]
		avail := int32(gpuQty.Value())
		if avail <= 0 {
			continue
		}
		capGPUs := workerGPUs
		if len(allocs) == 0 {
			capGPUs = leaderGPUs
		}
		if capGPUs <= 0 {
			continue
		}
		gpus := min(avail, capGPUs)
		allocs = append(allocs, workloadtypes.DegradedNodeAllocation{
			NodeName:     nodeName,
			Accelerators: gpus,
		})
		total += gpus
	}
	return allocs, total, nil
}

func formatDegradedNodes(nodes []workloadtypes.DegradedNodeAllocation) string {
	parts := make([]string, len(nodes))
	for i, n := range nodes {
		parts[i] = fmt.Sprintf("%s=%d", n.NodeName, n.Accelerators)
	}
	return strings.Join(parts, ",")
}
