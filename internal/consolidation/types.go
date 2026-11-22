// Package consolidation implements a read-only, offline feasibility
// simulation for node consolidation ("what if these nodes were removed").
//
// The engine never talks to a cluster, a cloud API, or the Kubernetes
// scheduler. It consumes an explicit snapshot (nodes, pods, constraints,
// prices) and reports whether the pods on the candidate-removal nodes
// could be placed onto the remaining nodes under the constraints it
// understands. It never calls eviction, cordon, delete, or cloud
// scale-down APIs. A feasible result only proves static feasibility under
// the supported constraints — it is not a promise of zero disruption
// during a real migration.
//
// The simulation input is deliberately standalone: collector.NodeInfo and
// collector.PodInfo do not carry node selectors, taints, tolerations, or
// volume binding data, and those must never be silently defaulted to
// "unrestricted". Callers either supply a fully specified SimulateInput or
// use InputFromCluster, which records every missing constraint dimension
// as an explicit data gap that surfaces in Result.Uncertainties.
package consolidation

// Resources is a capacity or request vector. CPU is in millicores, memory
// in bytes, Pods in pod slots, and Extended holds integer extended
// resources such as "nvidia.com/gpu". A zero-valued field means "no
// amount tracked", not "unlimited".
type Resources struct {
	MilliCPU    int64
	MemoryBytes int64
	Pods        int64
	Extended    map[string]int64
}

// Taint is a node taint. Only NoSchedule and NoExecute effects are hard
// constraints for placement; PreferNoSchedule is a preference and never
// blocks placement here.
type Taint struct {
	Key    string
	Value  string
	Effect string
}

// Toleration is a pod toleration. An empty Operator means "Equal",
// matching Kubernetes defaults.
type Toleration struct {
	Key      string
	Operator string // "Equal" or "Exists"
	Value    string
	Effect   string // empty matches all effects
}

// MatchExpression is one label matching expression. Supported operators
// are In, NotIn, Exists, DoesNotExist, Gt, and Lt. Any other operator
// makes the owning constraint undecidable and is reported, never ignored.
type MatchExpression struct {
	Key      string
	Operator string
	Values   []string
}

// PodAffinityTerm is a required pod anti-affinity term (only anti-affinity
// is modeled; pod affinity and preferred terms are not hard constraints).
// An empty MatchExpressions list matches all pods, like an empty
// metav1.LabelSelector. An empty Namespaces list means the pod's own
// namespace.
type PodAffinityTerm struct {
	MatchExpressions []MatchExpression
	TopologyKey      string
	Namespaces       []string
}

// TopologyTerm constrains a pod to nodes whose labels match every entry:
// label key must have one of the listed values. Terms are ORed, entries
// within a term are ANDed. Used for known volume topology constraints
// (StorageClass allowedTopologies or bound PV node affinity).
type TopologyTerm map[string][]string

// PDBInfo records a known PodDisruptionBudget. It is used only for risk
// notes; eviction sequencing is not simulated.
type PDBInfo struct {
	MinAvailable   *int32
	MaxUnavailable *int32
}

// PodSnapshot is one pod's scheduling-relevant state. Requests must be
// the pod's effective request (max of summed app containers vs. the
// largest init container), computed by the caller.
type PodSnapshot struct {
	Name      string
	Namespace string
	Labels    map[string]string
	Requests  Resources

	// OwnerKind is the resolved controller kind (e.g. "Deployment",
	// "StatefulSet", "DaemonSet"). Empty means a bare pod with no
	// controller. MirrorPod marks kubelet-managed static pods.
	OwnerKind string
	OwnerName string
	MirrorPod bool

	// LocalStorage marks pods using local, non-migratable storage
	// (emptyDir, hostPath, local PV). Such pods can never be relocated
	// by this simulation.
	LocalStorage bool

	// Hard scheduling constraints. Nil/empty means "no constraint" only
	// when the caller actually knows that; unknown constraint data must
	// be reported via Unsupported or SimulateInput.DataGaps instead.
	NodeSelector            map[string]string
	RequiredNodeAffinity    [][]MatchExpression // OR of terms, each term ANDed
	Tolerations             []Toleration
	RequiredPodAntiAffinity []PodAffinityTerm
	VolumeTopology          []TopologyTerm // OR of terms
	Unsupported             []string       // required scheduling semantics the engine does not model

	// PDB is advisory only: it produces a risk note, not a placement
	// constraint.
	PDB *PDBInfo
}

// NodeSnapshot is one node's scheduling-relevant state. MonthlyPrice is
// the known monthly cost; nil means the price is unknown — unknown prices
// are never treated as zero.
type NodeSnapshot struct {
	Name         string
	Labels       map[string]string
	Taints       []Taint
	Allocatable  Resources
	MonthlyPrice *float64
	Pods         []PodSnapshot // resident pods
}

// SimulateInput is the complete, self-contained simulation input.
type SimulateInput struct {
	Nodes []NodeSnapshot

	// RemoveNodes is the candidate removal set. All listed nodes are
	// validated jointly — removability of each node alone never implies
	// removability of the set.
	RemoveNodes []string

	// AddedResourceMonthlyCost is the explicit monthly cost of any new
	// resources the consolidation plan assumes (e.g. one larger node
	// replacing two smaller ones). It is subtracted from the removed
	// nodes' known cost. Defaults to zero.
	AddedResourceMonthlyCost float64

	// MaxPlacementSteps bounds the placement search. Zero selects
	// DefaultMaxPlacementSteps. If the bound is exhausted the result
	// status is StatusUnknown, never "infeasible".
	MaxPlacementSteps int

	// DataGaps lists constraint dimensions the snapshot is known to be
	// missing (e.g. "node taints not collected"). They are copied into
	// Result.Uncertainties so missing data is never mistaken for
	// "no constraint".
	DataGaps []string
}

// Status is the overall verdict of a simulation.
type Status string

const (
	// StatusFeasible means a complete placement was found for every
	// migratable pod under the supported constraints.
	StatusFeasible Status = "feasible"
	// StatusInfeasible means a deterministic blocker was found: a pod
	// that cannot be migrated at all, or a completed search that found
	// no placement. It is a statement about this model and these
	// constraints, not a proof of mathematical impossibility.
	StatusInfeasible Status = "infeasible"
	// StatusUnknown means the engine cannot certify either way: the
	// search budget was exhausted, the input was invalid, or pods depend
	// on scheduling semantics the engine does not model.
	StatusUnknown Status = "unknown"
)

// Placement records one pod's target node in the found plan.
type Placement struct {
	Pod      string // namespace/name
	FromNode string
	ToNode   string
}

// BlockedPod is a pod that cannot be migrated, with the reason.
type BlockedPod struct {
	Pod    string // namespace/name
	Node   string
	Reason string
}

// NodePlan is the resulting state of one remaining node.
type NodePlan struct {
	Node     string
	Incoming []string // pod keys placed onto this node
	Free     Resources
}

// SavingsEstimate is the conditional savings computation. NetMonthly is
// nil whenever any removed node's price is unknown — unknown prices are
// never treated as zero.
type SavingsEstimate struct {
	RemovedMonthlyKnown  float64
	UnknownPriceNodes    []string
	AddedResourceMonthly float64
	NetMonthly           *float64
	Note                 string
}

// SearchLimits records the search budget actually used.
type SearchLimits struct {
	MaxSteps  int
	StepsUsed int
	Exhausted bool
}

// Result is the full output of a simulation.
type Result struct {
	Status       Status
	RemovedNodes []string

	Placements []Placement
	Blocked    []BlockedPod
	// DroppedDaemonPods are DaemonSet replicas on removed nodes. They
	// are intentionally NOT relocated onto existing nodes: remaining
	// nodes already carry their own daemon overhead, and mechanically
	// copying daemon pods would double-count it.
	DroppedDaemonPods []string

	MovedPods int
	NodePlans []NodePlan

	Savings SavingsEstimate
	Limits  SearchLimits

	// Notes holds general findings (search outcome, daemon handling,
	// the static-feasibility disclaimer).
	Notes []string
	// RiskNotes holds known migration-execution risks (e.g. PDBs) that
	// do not change placement but must accompany any plan.
	RiskNotes []string
	// Uncertainties holds everything that makes the result conditional:
	// data gaps, unmodeled semantics, search-limit exhaustion.
	Uncertainties []string
}
