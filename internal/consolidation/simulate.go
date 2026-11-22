package consolidation

import (
	"fmt"
	"sort"
	"strings"
)

// DefaultMaxPlacementSteps bounds the placement search when the caller
// does not set MaxPlacementSteps.
const DefaultMaxPlacementSteps = 100000

const savingsNote = "Conditional estimate: known cost of removed nodes minus explicitly added " +
	"resource cost. It requires the removed nodes to actually be decommissioned, does not " +
	"double-count pod cost allocations, and does not assume the cloud bill drops immediately."

const staticFeasibilityNote = "A feasible result proves only static feasibility under the " +
	"supported constraints; it does not guarantee a disruption-free migration, and PDBs, " +
	"eviction ordering, and drain scheduling are not simulated."

// knownControllerKinds are controllers whose pods are recreated on other
// nodes after eviction. Anything else is treated as unknown and blocked.
var knownControllerKinds = map[string]bool{
	"Deployment":            true,
	"ReplicaSet":            true,
	"StatefulSet":           true,
	"ReplicationController": true,
	"Job":                   true,
	"CronJob":               true,
}

// podMeta is the scheduling-relevant view of a pod already considered
// placed (resident or incoming), used for anti-affinity checks.
type podMeta struct {
	namespace    string
	labels       map[string]string
	antiAffinity []PodAffinityTerm
}

type podState struct {
	snap     PodSnapshot
	key      string // namespace/name
	fromNode string
}

type nodeState struct {
	snap     *NodeSnapshot
	used     Resources
	resident []podMeta
	incoming []*podState
}

func (n *nodeState) allPods() []podMeta {
	out := make([]podMeta, 0, len(n.resident)+len(n.incoming))
	out = append(out, n.resident...)
	for _, p := range n.incoming {
		out = append(out, podMeta{
			namespace:    p.snap.Namespace,
			labels:       p.snap.Labels,
			antiAffinity: p.snap.RequiredPodAntiAffinity,
		})
	}
	return out
}

func podKey(namespace, name string) string { return namespace + "/" + name }

func cloneResources(r Resources) Resources {
	out := r
	if r.Extended != nil {
		out.Extended = make(map[string]int64, len(r.Extended))
		for k, v := range r.Extended {
			out.Extended[k] = v
		}
	}
	return out
}

func addResources(dst *Resources, r Resources) {
	dst.MilliCPU += r.MilliCPU
	dst.MemoryBytes += r.MemoryBytes
	dst.Pods += r.Pods
	if len(r.Extended) > 0 && dst.Extended == nil {
		dst.Extended = map[string]int64{}
	}
	for k, v := range r.Extended {
		dst.Extended[k] += v
	}
}

// podRequests returns the pod's capacity footprint: its effective
// requests plus one pod slot.
func podRequests(p PodSnapshot) Resources {
	r := cloneResources(p.Requests)
	r.Pods += 1
	return r
}

// fits reports whether the node's remaining allocatable can absorb the
// request. A zero Allocatable.Pods means the pod-count limit is unknown;
// the pod-count check is then skipped (and flagged as a data gap by the
// caller), never treated as unlimited-by-design.
func fits(allocatable, used, req Resources, checkPodCount bool) bool {
	if used.MilliCPU+req.MilliCPU > allocatable.MilliCPU {
		return false
	}
	if used.MemoryBytes+req.MemoryBytes > allocatable.MemoryBytes {
		return false
	}
	if checkPodCount && used.Pods+req.Pods > allocatable.Pods {
		return false
	}
	for k, v := range req.Extended {
		if used.Extended[k]+v > allocatable.Extended[k] {
			return false
		}
	}
	return true
}

// Simulate runs the offline consolidation feasibility check. It never
// mutates its input and never touches a cluster or cloud API.
func Simulate(in SimulateInput) Result {
	res := Result{
		Savings: SavingsEstimate{
			AddedResourceMonthly: in.AddedResourceMonthlyCost,
			Note:                 savingsNote,
		},
	}
	res.Uncertainties = append(res.Uncertainties, in.DataGaps...)

	maxSteps := in.MaxPlacementSteps
	if maxSteps <= 0 {
		maxSteps = DefaultMaxPlacementSteps
	}
	res.Limits.MaxSteps = maxSteps

	// Canonicalize node order so results do not depend on input order.
	nodes := make([]NodeSnapshot, len(in.Nodes))
	copy(nodes, in.Nodes)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })

	seen := map[string]bool{}
	for i := range nodes {
		if seen[nodes[i].Name] {
			res.Status = StatusUnknown
			res.Uncertainties = append(res.Uncertainties,
				fmt.Sprintf("invalid input: duplicate node name %q", nodes[i].Name))
			return res
		}
		seen[nodes[i].Name] = true
	}

	if len(in.RemoveNodes) == 0 {
		res.Status = StatusUnknown
		res.Uncertainties = append(res.Uncertainties, "invalid input: no candidate removal nodes specified")
		return res
	}
	removeSet := map[string]bool{}
	for _, name := range in.RemoveNodes {
		removeSet[name] = true
	}
	for name := range removeSet {
		if !seen[name] {
			res.Status = StatusUnknown
			res.Uncertainties = append(res.Uncertainties,
				fmt.Sprintf("invalid input: candidate removal node %q is not in the snapshot", name))
			return res
		}
		res.RemovedNodes = append(res.RemovedNodes, name)
	}
	sort.Strings(res.RemovedNodes)

	// Build remaining-node states with resident usage accounted.
	var remaining []*nodeState
	var removed []*NodeSnapshot
	for i := range nodes {
		if removeSet[nodes[i].Name] {
			removed = append(removed, &nodes[i])
			continue
		}
		ns := &nodeState{snap: &nodes[i], used: Resources{Extended: map[string]int64{}}}
		for _, p := range nodes[i].Pods {
			addResources(&ns.used, podRequests(p))
			ns.resident = append(ns.resident, podMeta{
				namespace:    p.Namespace,
				labels:       p.Labels,
				antiAffinity: p.RequiredPodAntiAffinity,
			})
		}
		if nodes[i].Allocatable.Pods <= 0 {
			res.Uncertainties = append(res.Uncertainties, fmt.Sprintf(
				"node %s: pod-count limit not provided; pod-count capacity was not verified", nodes[i].Name))
		}
		remaining = append(remaining, ns)
	}

	// Classify pods on removed nodes.
	var migratable []*podState
	uncertain := false
	for _, rn := range removed {
		pods := make([]PodSnapshot, len(rn.Pods))
		copy(pods, rn.Pods)
		sort.Slice(pods, func(i, j int) bool {
			if pods[i].Namespace != pods[j].Namespace {
				return pods[i].Namespace < pods[j].Namespace
			}
			return pods[i].Name < pods[j].Name
		})
		for _, p := range pods {
			key := podKey(p.Namespace, p.Name)
			switch {
			case p.MirrorPod:
				res.Blocked = append(res.Blocked, BlockedPod{Pod: key, Node: rn.Name,
					Reason: "mirror pod is managed by the kubelet and cannot be rescheduled by a controller"})
			case p.OwnerKind == "DaemonSet":
				res.DroppedDaemonPods = append(res.DroppedDaemonPods, key)
			case p.OwnerKind == "":
				res.Blocked = append(res.Blocked, BlockedPod{Pod: key, Node: rn.Name,
					Reason: "bare pod has no controller; it will not be recreated on another node"})
			case p.LocalStorage:
				res.Blocked = append(res.Blocked, BlockedPod{Pod: key, Node: rn.Name,
					Reason: "uses local, non-migratable storage"})
			case len(p.Unsupported) > 0:
				res.Blocked = append(res.Blocked, BlockedPod{Pod: key, Node: rn.Name,
					Reason: "requires scheduling semantics the simulation does not model: " +
						strings.Join(p.Unsupported, ", ")})
				uncertain = true
			case !knownControllerKinds[p.OwnerKind]:
				res.Blocked = append(res.Blocked, BlockedPod{Pod: key, Node: rn.Name,
					Reason: fmt.Sprintf("unknown controller kind %q; rescheduling behavior is unknown", p.OwnerKind)})
			default:
				migratable = append(migratable, &podState{snap: p, key: key, fromNode: rn.Name})
			}
		}
	}
	if len(res.DroppedDaemonPods) > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"%d DaemonSet replica(s) on removed nodes were not relocated: remaining nodes already carry their own daemon overhead",
			len(res.DroppedDaemonPods)))
	}

	// Fixed pod ordering: largest first (memory, then CPU), name as
	// tie-break — deterministic regardless of input order.
	sort.Slice(migratable, func(i, j int) bool {
		a, b := migratable[i].snap.Requests, migratable[j].snap.Requests
		if a.MemoryBytes != b.MemoryBytes {
			return a.MemoryBytes > b.MemoryBytes
		}
		if a.MilliCPU != b.MilliCPU {
			return a.MilliCPU > b.MilliCPU
		}
		return migratable[i].key < migratable[j].key
	})

	// Constraint-only pre-check: a pod no remaining node can ever take is
	// a deterministic blocker, independent of capacity.
	var searchable []*podState
	for _, p := range migratable {
		reasons, err := constraintBlockReasons(p, remaining)
		if err != nil {
			res.Blocked = append(res.Blocked, BlockedPod{Pod: p.key, Node: p.fromNode, Reason: err.Error()})
			uncertain = true
			continue
		}
		if len(reasons) > 0 {
			res.Blocked = append(res.Blocked, BlockedPod{Pod: p.key, Node: p.fromNode,
				Reason: strings.Join(reasons, "; ")})
			continue
		}
		searchable = append(searchable, p)
	}

	// Capacity-aware placement search over the remaining pods.
	searchFailed := false
	if len(searchable) > 0 {
		s := &searcher{pods: searchable, nodes: remaining, max: maxSteps}
		if s.place(0) {
			res.Limits.StepsUsed = s.steps
			for _, n := range remaining {
				for _, p := range n.incoming {
					res.Placements = append(res.Placements, Placement{Pod: p.key, FromNode: p.fromNode, ToNode: n.snap.Name})
				}
			}
			sort.Slice(res.Placements, func(i, j int) bool { return res.Placements[i].Pod < res.Placements[j].Pod })
		} else {
			res.Limits.StepsUsed = s.steps
			if s.exhausted {
				res.Limits.Exhausted = true
				res.Uncertainties = append(res.Uncertainties, fmt.Sprintf(
					"placement search exhausted its step budget (%d); no plan found, which is not a proof of infeasibility", s.max))
			} else {
				searchFailed = true
				res.Notes = append(res.Notes, fmt.Sprintf(
					"no feasible placement found for %d pod(s) under the supported constraints "+
						"(search completed within the step limit; this is a model result, not a proof of mathematical impossibility)",
					len(searchable)))
			}
		}
	}

	// Node plans for remaining nodes.
	for _, n := range remaining {
		plan := NodePlan{Node: n.snap.Name, Free: freeResources(n)}
		for _, p := range n.incoming {
			plan.Incoming = append(plan.Incoming, p.key)
		}
		res.NodePlans = append(res.NodePlans, plan)
	}
	res.MovedPods = len(res.Placements)

	// Savings: known removed-node cost minus explicit added cost; unknown
	// prices are listed and never treated as zero.
	for _, rn := range removed {
		if rn.MonthlyPrice == nil {
			res.Savings.UnknownPriceNodes = append(res.Savings.UnknownPriceNodes, rn.Name)
			continue
		}
		res.Savings.RemovedMonthlyKnown += *rn.MonthlyPrice
	}
	if len(res.Savings.UnknownPriceNodes) == 0 {
		net := res.Savings.RemovedMonthlyKnown - res.Savings.AddedResourceMonthly
		res.Savings.NetMonthly = &net
	} else {
		res.Uncertainties = append(res.Uncertainties,
			"monthly price unknown for removed node(s): "+strings.Join(res.Savings.UnknownPriceNodes, ", ")+
				"; net savings cannot be computed")
	}

	// Risk notes from known PDBs (advisory only).
	for _, p := range migratable {
		if p.snap.PDB == nil {
			continue
		}
		var detail []string
		if p.snap.PDB.MinAvailable != nil {
			detail = append(detail, fmt.Sprintf("minAvailable=%d", *p.snap.PDB.MinAvailable))
		}
		if p.snap.PDB.MaxUnavailable != nil {
			detail = append(detail, fmt.Sprintf("maxUnavailable=%d", *p.snap.PDB.MaxUnavailable))
		}
		res.RiskNotes = append(res.RiskNotes, fmt.Sprintf(
			"%s is covered by a PodDisruptionBudget (%s); eviction sequencing is not simulated",
			p.key, strings.Join(detail, ", ")))
	}
	res.Notes = append(res.Notes, staticFeasibilityNote)

	// Overall status.
	switch {
	case uncertain:
		res.Status = StatusUnknown
	case len(res.Blocked) > 0:
		res.Status = StatusInfeasible
	case res.Limits.Exhausted:
		res.Status = StatusUnknown
	case searchFailed:
		res.Status = StatusInfeasible
	default:
		res.Status = StatusFeasible
	}
	return res
}

// constraintBlockReasons returns the hard-constraint categories that no
// remaining node satisfies for this pod. An empty result means at least
// one node passes all hard constraints (capacity is checked separately).
func constraintBlockReasons(p *podState, remaining []*nodeState) ([]string, error) {
	if len(remaining) == 0 {
		return []string{"no remaining nodes to place on"}, nil
	}
	selectorBlocked, affinityBlocked, taintBlocked, volumeBlocked := true, true, true, true
	for _, n := range remaining {
		if len(nodeSelectorFailures(p.snap.NodeSelector, n.snap.Labels)) == 0 {
			selectorBlocked = false
		}
		ok, err := nodeAffinitySatisfied(p.snap.RequiredNodeAffinity, n.snap.Labels)
		if err != nil {
			return nil, fmt.Errorf("pod has an undecidable scheduling constraint: %v", err)
		}
		if ok {
			affinityBlocked = false
		}
		if len(untoleratedTaints(p.snap.Tolerations, n.snap.Taints)) == 0 {
			taintBlocked = false
		}
		if volumeTopologySatisfied(p.snap.VolumeTopology, n.snap.Labels) {
			volumeBlocked = false
		}
	}
	var reasons []string
	if selectorBlocked {
		reasons = append(reasons, "nodeSelector is not satisfied by any remaining node")
	}
	if affinityBlocked {
		reasons = append(reasons, "required node affinity is not satisfied by any remaining node")
	}
	if taintBlocked {
		reasons = append(reasons, "every remaining node has untolerated taints")
	}
	if volumeBlocked {
		reasons = append(reasons, "volume topology constraint is not satisfied by any remaining node")
	}
	return reasons, nil
}

func freeResources(n *nodeState) Resources {
	free := Resources{Extended: map[string]int64{}}
	free.MilliCPU = n.snap.Allocatable.MilliCPU - n.used.MilliCPU
	free.MemoryBytes = n.snap.Allocatable.MemoryBytes - n.used.MemoryBytes
	free.Pods = n.snap.Allocatable.Pods - n.used.Pods
	for k, v := range n.snap.Allocatable.Extended {
		free.Extended[k] = v - n.used.Extended[k]
	}
	for k, v := range n.used.Extended {
		if _, ok := n.snap.Allocatable.Extended[k]; !ok {
			free.Extended[k] = -v
		}
	}
	return free
}

// searcher is a deterministic bounded backtracking placement search.
// Pods and nodes are tried in fixed sorted order; identical inputs always
// produce identical plans.
type searcher struct {
	pods      []*podState
	nodes     []*nodeState
	steps     int
	max       int
	exhausted bool
}

func (s *searcher) place(idx int) bool {
	if s.exhausted {
		return false
	}
	if idx == len(s.pods) {
		return true
	}
	p := s.pods[idx]
	for _, n := range s.nodes {
		s.steps++
		if s.steps > s.max {
			s.exhausted = true
			return false
		}
		failures, err := nodeConstraintFailures(p, n)
		if err != nil || len(failures) > 0 {
			continue
		}
		if !fits(n.snap.Allocatable, n.used, podRequests(p.snap), n.snap.Allocatable.Pods > 0) {
			continue
		}
		ok, err := antiAffinityOK(p, n, s.nodes)
		if err != nil || !ok {
			continue
		}
		req := podRequests(p.snap)
		addResources(&n.used, req)
		n.incoming = append(n.incoming, p)
		if s.place(idx + 1) {
			return true
		}
		// Backtrack.
		for k, v := range req.Extended {
			n.used.Extended[k] -= v
		}
		n.used.MilliCPU -= req.MilliCPU
		n.used.MemoryBytes -= req.MemoryBytes
		n.used.Pods -= req.Pods
		n.incoming = n.incoming[:len(n.incoming)-1]
	}
	return false
}
