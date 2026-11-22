package consolidation

import (
	"github.com/tanrikuluozlem/burn/internal/collector"
)

// InputFromCluster builds a SimulateInput from the existing collector
// snapshot. The collector does not gather node taints, pod
// nodeSelector/affinity, tolerations, volume bindings, or per-node pod
// limits, so those dimensions are recorded as explicit data gaps — they
// surface in Result.Uncertainties instead of being silently treated as
// "no constraint". Prices come from the caller (e.g. analyzer.NodeCost);
// nodes missing from the prices map keep an unknown (nil) price, which
// the engine never treats as zero.
func InputFromCluster(ci *collector.ClusterInfo, remove []string, prices map[string]float64) SimulateInput {
	in := SimulateInput{
		RemoveNodes: remove,
		DataGaps: []string{
			"snapshot built from collector data lacks node taints, pod nodeSelector/nodeAffinity, " +
				"tolerations, and volume binding information; feasibility is conditional on the " +
				"absence of such constraints",
		},
	}

	// Index workloads by namespace/name for local-storage and PDB data.
	type wlKey struct{ ns, name string }
	workloads := map[wlKey]collector.WorkloadInfo{}
	for _, w := range ci.Workloads {
		workloads[wlKey{w.Namespace, w.Name}] = w
	}

	for _, n := range ci.Nodes {
		node := NodeSnapshot{
			Name:   n.Name,
			Labels: n.Labels,
			Allocatable: Resources{
				MilliCPU:    n.CPUAllocatable,
				MemoryBytes: n.MemAllocatable,
				// Pod-count limit is not collected; 0 marks it unknown
				// and the engine reports the gap.
				Pods: 0,
			},
		}
		if n.GPUCount > 0 {
			node.Allocatable.Extended = map[string]int64{"nvidia.com/gpu": n.GPUCount}
		}
		if price, ok := prices[n.Name]; ok {
			p := price
			node.MonthlyPrice = &p
		}
		for _, p := range n.Pods {
			pod := PodSnapshot{
				Name:      p.Name,
				Namespace: p.Namespace,
				OwnerKind: p.OwnerKind,
				OwnerName: p.OwnerName,
				Requests: Resources{
					MilliCPU:    p.CPURequest,
					MemoryBytes: p.MemoryRequest,
				},
			}
			if p.GPURequest > 0 {
				pod.Requests.Extended = map[string]int64{"nvidia.com/gpu": p.GPURequest}
			}
			if w, ok := workloads[wlKey{p.Namespace, p.OwnerName}]; ok {
				pod.LocalStorage = w.HasLocalStorage
				if w.PDBFound {
					pdb := &PDBInfo{}
					if w.PDBUsesMaxUnavailable {
						v := w.PDBMaxUnavailable
						pdb.MaxUnavailable = &v
					} else {
						v := w.PDBMinAvailable
						pdb.MinAvailable = &v
					}
					pod.PDB = pdb
				}
			}
			node.Pods = append(node.Pods, pod)
		}
		in.Nodes = append(in.Nodes, node)
	}
	return in
}
