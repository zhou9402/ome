package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// AutoDegradePolicy configures automatic in-place heterogeneous DEP degradation
// when a multi-node Instance cannot schedule at full GPU capacity.
type AutoDegradePolicy struct {
	// Enabled turns on automatic in-place heterogeneous degradation.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MinTotalAccelerators is the minimum total accelerator count across an
	// Instance below which OME refuses to degrade further.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinTotalAccelerators int32 `json:"minTotalAccelerators,omitempty"`

	// TriggerTimeoutSeconds is how long an Instance must stay Pending or
	// Unschedulable before OME inspects existing nodes and triggers an in-place
	// degraded rebuild. Defaults to 60 when unset or <= 0.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TriggerTimeoutSeconds int32 `json:"triggerTimeoutSeconds,omitempty"`

	// CooldownSeconds is the minimum time between degraded topology transitions
	// for an Instance. Defaults to 600 when unset or <= 0.
	// +optional
	// +kubebuilder:validation:Minimum=0
	CooldownSeconds int32 `json:"cooldownSeconds,omitempty"`
}

// DegradedNodeAllocation describes the pinned node and accelerator count for
// one pod rank in an automatically degraded Instance.
type DegradedNodeAllocation struct {
	// NodeName is the Kubernetes Node hostname the pod is pinned to via
	// required NodeAffinity.
	// +kubebuilder:validation:MinLength=1
	NodeName string `json:"nodeName"`

	// Accelerators is the nvidia.com/gpu count allocated to this pod on
	// NodeName.
	// +kubebuilder:validation:Minimum=1
	Accelerators int32 `json:"accelerators"`
}

// DegradedTopology records the active automatic in-place heterogeneous
// accelerator layout for one Instance in InferenceReplica.Status.
type DegradedTopology struct {
	// TotalAccelerators is the sum of Accelerators across Nodes.
	// +kubebuilder:validation:Minimum=1
	TotalAccelerators int32 `json:"totalAccelerators"`

	// LastTransitionTime records when this degraded topology was committed,
	// used to enforce AutoDegradePolicy.CooldownSeconds.
	// +optional
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`

	// Nodes lists the per-pod node pins and accelerator counts in flat pod rank
	// order: nodes[0] is rendered as leader (offset 0) and nodes[1..] as
	// worker-0..k.
	// +optional
	// +listType=atomic
	Nodes []DegradedNodeAllocation `json:"nodes,omitempty"`
}
