package consolidation

import (
	"strings"
	"testing"

	"github.com/tanrikuluozlem/burn/internal/collector"
)

func TestInputFromCluster(t *testing.T) {
	ci := &collector.ClusterInfo{
		Nodes: []collector.NodeInfo{
			{
				Name:           "a",
				Labels:         map[string]string{"pool": "default"},
				CPUAllocatable: 2000,
				MemAllocatable: 4096,
				Pods: []collector.PodInfo{
					{Name: "web-1", Namespace: "app", OwnerKind: "Deployment", OwnerName: "web",
						CPURequest: 500, MemoryRequest: 512},
					{Name: "agent-x", Namespace: "kube-system", OwnerKind: "DaemonSet", OwnerName: "agent",
						CPURequest: 100, MemoryRequest: 128},
				},
			},
			{
				Name:           "b",
				CPUAllocatable: 4000,
				MemAllocatable: 8192,
				GPUCount:       2,
			},
		},
		Workloads: []collector.WorkloadInfo{
			{Namespace: "app", Name: "web", Kind: "Deployment", HasLocalStorage: true},
		},
	}

	in := InputFromCluster(ci, []string{"a"}, map[string]float64{"a": 100})
	if len(in.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(in.Nodes))
	}
	if len(in.DataGaps) == 0 {
		t.Error("DataGaps is empty; collector-built input must record missing constraint dimensions")
	}

	var a, b NodeSnapshot
	for _, n := range in.Nodes {
		switch n.Name {
		case "a":
			a = n
		case "b":
			b = n
		}
	}
	if a.MonthlyPrice == nil || *a.MonthlyPrice != 100 {
		t.Errorf("node a price = %v, want 100", a.MonthlyPrice)
	}
	if b.MonthlyPrice != nil {
		t.Errorf("node b price = %v, want nil (unknown)", *b.MonthlyPrice)
	}
	if b.Allocatable.Extended["nvidia.com/gpu"] != 2 {
		t.Errorf("node b GPUs = %d, want 2", b.Allocatable.Extended["nvidia.com/gpu"])
	}
	if a.Pods[0].Requests.MilliCPU != 500 || a.Pods[0].Requests.MemoryBytes != 512 {
		t.Errorf("pod requests = %+v, want 500m/512", a.Pods[0].Requests)
	}
	if !a.Pods[0].LocalStorage {
		t.Error("web-1 LocalStorage = false, want true (from WorkloadInfo)")
	}

	// The local-storage pod blocks; the daemon pod is dropped, not moved.
	res := Simulate(in)
	if res.Status != StatusInfeasible {
		t.Fatalf("Status = %s, want %s (local-storage pod blocks)", res.Status, StatusInfeasible)
	}
	if len(res.DroppedDaemonPods) != 1 {
		t.Errorf("DroppedDaemonPods = %v, want one entry", res.DroppedDaemonPods)
	}
	joined := strings.Join(res.Uncertainties, "\n")
	if !strings.Contains(joined, "taints") {
		t.Errorf("Uncertainties missing collector data-gap note: %v", res.Uncertainties)
	}
	if !strings.Contains(joined, "pod-count") {
		t.Errorf("Uncertainties missing pod-count gap note: %v", res.Uncertainties)
	}
	// Only node a is removed and its price is known: net = 100 - 0.
	if res.Savings.NetMonthly == nil || *res.Savings.NetMonthly != 100 {
		t.Errorf("NetMonthly = %v, want 100", res.Savings.NetMonthly)
	}
}
