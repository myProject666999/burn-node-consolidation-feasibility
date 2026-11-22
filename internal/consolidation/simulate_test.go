package consolidation

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func f64(v float64) *float64 { return &v }

func i32(v int32) *int32 { return &v }

// pod builds a minimal Deployment-owned pod with the given requests.
func pod(ns, name string, milliCPU, memBytes int64) PodSnapshot {
	return PodSnapshot{
		Name:      name,
		Namespace: ns,
		OwnerKind: "Deployment",
		OwnerName: name + "-deploy",
		Requests:  Resources{MilliCPU: milliCPU, MemoryBytes: memBytes},
	}
}

func node(name string, milliCPU, memBytes int64, price *float64, pods ...PodSnapshot) NodeSnapshot {
	return NodeSnapshot{
		Name:         name,
		Labels:       map[string]string{"kubernetes.io/hostname": name},
		Allocatable:  Resources{MilliCPU: milliCPU, MemoryBytes: memBytes},
		MonthlyPrice: price,
		Pods:         pods,
	}
}

func placementsByPod(res Result) map[string]string {
	out := map[string]string{}
	for _, p := range res.Placements {
		out[p.Pod] = p.ToNode
	}
	return out
}

// Counterexample from the design: two remaining nodes each have 1 core
// free; the pod to migrate needs 2 cores. Aggregate idle (2 cores) is
// enough, but no single node fits the pod.
func TestAggregateIdleDoesNotImplyFeasible(t *testing.T) {
	in := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), pod("app", "big", 2000, 1024)),
			node("b", 2000, 4096, f64(100), pod("sys", "rb", 1000, 1024)),
			node("c", 2000, 4096, f64(100), pod("sys", "rc", 1000, 1024)),
		},
		RemoveNodes: []string{"a"},
	}
	res := Simulate(in)
	if res.Status != StatusInfeasible {
		t.Fatalf("Status = %s, want %s (2x1 core free cannot absorb one 2-core pod)", res.Status, StatusInfeasible)
	}
	if len(res.Placements) != 0 {
		t.Errorf("Placements = %v, want none", res.Placements)
	}
	if res.MovedPods != 0 {
		t.Errorf("MovedPods = %d, want 0", res.MovedPods)
	}
}

func TestFeasibleSinglePodMove(t *testing.T) {
	in := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), pod("app", "web", 500, 512)),
			node("b", 2000, 4096, f64(100), pod("sys", "rb", 1000, 1024)),
		},
		RemoveNodes:              []string{"a"},
		AddedResourceMonthlyCost: 10,
	}
	res := Simulate(in)
	if res.Status != StatusFeasible {
		t.Fatalf("Status = %s, want %s (blocked: %+v, notes: %v)", res.Status, StatusFeasible, res.Blocked, res.Notes)
	}
	if res.MovedPods != 1 {
		t.Fatalf("MovedPods = %d, want 1", res.MovedPods)
	}
	got := placementsByPod(res)
	if got["app/web"] != "b" {
		t.Errorf("app/web placed on %q, want %q", got["app/web"], "b")
	}
	if res.Savings.NetMonthly == nil {
		t.Fatal("NetMonthly is nil, want 90 (100 removed - 10 added)")
	}
	if *res.Savings.NetMonthly != 90 {
		t.Errorf("NetMonthly = %.2f, want 90", *res.Savings.NetMonthly)
	}
	// Source node must not appear as a placement target.
	for _, p := range res.Placements {
		if p.ToNode == "a" {
			t.Errorf("placement targets removed node: %+v", p)
		}
	}
}

func TestGPUExtendedResource(t *testing.T) {
	gpuPod := pod("ml", "trainer", 1000, 2048)
	gpuPod.Requests.Extended = map[string]int64{"nvidia.com/gpu": 1}

	// Remaining node has no GPU: cannot absorb.
	noGPU := SimulateInput{
		Nodes: []NodeSnapshot{
			node("gpu-a", 4000, 8192, f64(500), gpuPod),
			node("cpu-b", 8000, 16384, f64(200)),
		},
		RemoveNodes: []string{"gpu-a"},
	}
	noGPU.Nodes[0].Allocatable.Extended = map[string]int64{"nvidia.com/gpu": 2}
	res := Simulate(noGPU)
	if res.Status != StatusInfeasible {
		t.Fatalf("no-GPU: Status = %s, want %s", res.Status, StatusInfeasible)
	}

	// Add a GPU node: placement must land there, and only there.
	withGPU := SimulateInput{
		Nodes: []NodeSnapshot{
			node("gpu-a", 4000, 8192, f64(500), gpuPod),
			node("cpu-b", 8000, 16384, f64(200)),
			node("gpu-c", 4000, 8192, f64(500)),
		},
		RemoveNodes: []string{"gpu-a"},
	}
	withGPU.Nodes[0].Allocatable.Extended = map[string]int64{"nvidia.com/gpu": 2}
	withGPU.Nodes[2].Allocatable.Extended = map[string]int64{"nvidia.com/gpu": 2}
	res = Simulate(withGPU)
	if res.Status != StatusFeasible {
		t.Fatalf("with-GPU: Status = %s, want %s", res.Status, StatusFeasible)
	}
	if got := placementsByPod(res)["ml/trainer"]; got != "gpu-c" {
		t.Errorf("ml/trainer placed on %q, want gpu-c", got)
	}
	// GPU accounting on the target: 1 of 2 GPUs now used.
	for _, plan := range res.NodePlans {
		if plan.Node == "gpu-c" && plan.Free.Extended["nvidia.com/gpu"] != 1 {
			t.Errorf("gpu-c free GPUs = %d, want 1", plan.Free.Extended["nvidia.com/gpu"])
		}
	}
}

func TestRequiredPodAntiAffinity(t *testing.T) {
	anti := []PodAffinityTerm{{
		MatchExpressions: []MatchExpression{{Key: "app", Operator: "In", Values: []string{"web"}}},
		TopologyKey:      "kubernetes.io/hostname",
	}}
	mkPod := func(name string) PodSnapshot {
		p := pod("app", name, 500, 512)
		p.Labels = map[string]string{"app": "web"}
		p.RequiredPodAntiAffinity = anti
		return p
	}

	// One remaining node: two replicas cannot coexist.
	one := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), mkPod("web-1"), mkPod("web-2")),
			node("b", 4000, 8192, f64(100)),
		},
		RemoveNodes: []string{"a"},
	}
	res := Simulate(one)
	if res.Status != StatusInfeasible {
		t.Fatalf("one-node: Status = %s, want %s", res.Status, StatusInfeasible)
	}

	// Two remaining nodes: replicas must land on different nodes.
	two := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), mkPod("web-1"), mkPod("web-2")),
			node("b", 4000, 8192, f64(100)),
			node("c", 4000, 8192, f64(100)),
		},
		RemoveNodes: []string{"a"},
	}
	res = Simulate(two)
	if res.Status != StatusFeasible {
		t.Fatalf("two-node: Status = %s, want %s", res.Status, StatusFeasible)
	}
	got := placementsByPod(res)
	if got["app/web-1"] == got["app/web-2"] {
		t.Errorf("anti-affinity violated: both pods on %q", got["app/web-1"])
	}

	// Reverse direction: a resident pod's anti-affinity blocks the
	// incoming pod even though the incoming pod itself has no terms.
	resident := pod("app", "web-existing", 500, 512)
	resident.Labels = map[string]string{"app": "web"}
	resident.RequiredPodAntiAffinity = anti
	incoming := pod("app", "web-new", 500, 512)
	incoming.Labels = map[string]string{"app": "web"}
	reverse := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), incoming),
			node("b", 4000, 8192, f64(100), resident),
		},
		RemoveNodes: []string{"a"},
	}
	res = Simulate(reverse)
	if res.Status != StatusInfeasible {
		t.Fatalf("reverse: Status = %s, want %s (resident pod's anti-affinity must block)", res.Status, StatusInfeasible)
	}
}

func TestVolumeTopologyConstraint(t *testing.T) {
	p := pod("db", "pg-0", 1000, 2048)
	p.VolumeTopology = []TopologyTerm{{"topology.kubernetes.io/zone": {"zone-a"}}}

	zoneNode := func(name, zone string) NodeSnapshot {
		n := node(name, 4000, 8192, f64(100))
		n.Labels["topology.kubernetes.io/zone"] = zone
		return n
	}

	// Only zone-b remains: the pinned volume cannot follow the pod.
	res := Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			func() NodeSnapshot { n := zoneNode("a", "zone-a"); n.Pods = []PodSnapshot{p}; return n }(),
			zoneNode("b", "zone-b"),
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusInfeasible {
		t.Fatalf("Status = %s, want %s", res.Status, StatusInfeasible)
	}
	if len(res.Blocked) != 1 || !strings.Contains(res.Blocked[0].Reason, "volume topology") {
		t.Errorf("Blocked = %+v, want one volume-topology blocker", res.Blocked)
	}

	// A zone-a node remains: feasible, and the pod must land there.
	res = Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			func() NodeSnapshot { n := zoneNode("a", "zone-a"); n.Pods = []PodSnapshot{p}; return n }(),
			zoneNode("b", "zone-b"),
			zoneNode("c", "zone-a"),
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusFeasible {
		t.Fatalf("Status = %s, want %s", res.Status, StatusFeasible)
	}
	if got := placementsByPod(res)["db/pg-0"]; got != "c" {
		t.Errorf("db/pg-0 placed on %q, want c (only zone-a survivor)", got)
	}
}

func TestDaemonSetOnlyNode(t *testing.T) {
	ds := pod("kube-system", "agent-abc", 100, 128)
	ds.OwnerKind = "DaemonSet"
	ds.OwnerName = "agent"
	in := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), ds),
			node("b", 2000, 4096, f64(100), pod("app", "web", 500, 512)),
		},
		RemoveNodes: []string{"a"},
	}
	res := Simulate(in)
	if res.Status != StatusFeasible {
		t.Fatalf("Status = %s, want %s (daemon-only node drains trivially)", res.Status, StatusFeasible)
	}
	if res.MovedPods != 0 {
		t.Errorf("MovedPods = %d, want 0", res.MovedPods)
	}
	if len(res.DroppedDaemonPods) != 1 || res.DroppedDaemonPods[0] != "kube-system/agent-abc" {
		t.Errorf("DroppedDaemonPods = %v, want [kube-system/agent-abc]", res.DroppedDaemonPods)
	}
	// The daemon pod must not be mechanically copied onto node b.
	for _, plan := range res.NodePlans {
		if len(plan.Incoming) != 0 {
			t.Errorf("node %s incoming = %v, want none", plan.Node, plan.Incoming)
		}
	}
	if res.Savings.NetMonthly == nil || *res.Savings.NetMonthly != 100 {
		t.Errorf("NetMonthly = %v, want 100", res.Savings.NetMonthly)
	}
}

func TestBlockedPodCategories(t *testing.T) {
	bare := pod("app", "bare", 100, 128)
	bare.OwnerKind = ""
	bare.OwnerName = ""

	mirror := pod("kube-system", "static-api", 100, 128)
	mirror.OwnerKind = ""
	mirror.MirrorPod = true

	local := pod("app", "local-vol", 100, 128)
	local.LocalStorage = true

	weird := pod("app", "crontab-x", 100, 128)
	weird.OwnerKind = "MyCustomCRD"

	in := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 4000, 8192, f64(100), bare, mirror, local, weird),
			node("b", 8000, 16384, f64(100)),
		},
		RemoveNodes: []string{"a"},
	}
	res := Simulate(in)
	if res.Status != StatusInfeasible {
		t.Fatalf("Status = %s, want %s", res.Status, StatusInfeasible)
	}
	if len(res.Blocked) != 4 {
		t.Fatalf("Blocked = %d, want 4: %+v", len(res.Blocked), res.Blocked)
	}
	reasons := map[string]string{}
	for _, b := range res.Blocked {
		reasons[b.Pod] = b.Reason
	}
	if !strings.Contains(reasons["app/bare"], "no controller") {
		t.Errorf("bare pod reason: %q", reasons["app/bare"])
	}
	if !strings.Contains(reasons["kube-system/static-api"], "mirror pod") {
		t.Errorf("mirror pod reason: %q", reasons["kube-system/static-api"])
	}
	if !strings.Contains(reasons["app/local-vol"], "local") {
		t.Errorf("local storage reason: %q", reasons["app/local-vol"])
	}
	if !strings.Contains(reasons["app/crontab-x"], "unknown controller") {
		t.Errorf("unknown controller reason: %q", reasons["app/crontab-x"])
	}
}

func TestUnsupportedSemanticsMakeResultUnknown(t *testing.T) {
	p := pod("app", "special", 100, 128)
	p.Unsupported = []string{"custom scheduler foo"}
	res := Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), p),
			node("b", 4000, 8192, f64(100)),
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusUnknown {
		t.Fatalf("Status = %s, want %s for unmodeled scheduling semantics", res.Status, StatusUnknown)
	}
	if len(res.Blocked) != 1 {
		t.Errorf("Blocked = %+v, want one entry", res.Blocked)
	}
}

func TestTaintsAndTolerations(t *testing.T) {
	tainted := node("b", 4000, 8192, f64(100))
	tainted.Taints = []Taint{{Key: "dedicated", Value: "gpu", Effect: "NoSchedule"}}

	// Pod without toleration: no remaining node can take it.
	res := Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), pod("app", "web", 500, 512)),
			tainted,
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusInfeasible {
		t.Fatalf("no-toleration: Status = %s, want %s", res.Status, StatusInfeasible)
	}
	if !strings.Contains(res.Blocked[0].Reason, "untolerated taints") {
		t.Errorf("reason = %q, want untolerated taints", res.Blocked[0].Reason)
	}

	// Pod with matching toleration: feasible.
	tolerant := pod("app", "web", 500, 512)
	tolerant.Tolerations = []Toleration{{Key: "dedicated", Operator: "Equal", Value: "gpu", Effect: "NoSchedule"}}
	res = Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), tolerant),
			tainted,
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusFeasible {
		t.Fatalf("toleration: Status = %s, want %s", res.Status, StatusFeasible)
	}
}

func TestNodeSelectorConstraint(t *testing.T) {
	p := pod("app", "web", 500, 512)
	p.NodeSelector = map[string]string{"disk": "ssd"}
	res := Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), p),
			node("b", 4000, 8192, f64(100)), // no disk=ssd label
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusInfeasible {
		t.Fatalf("Status = %s, want %s", res.Status, StatusInfeasible)
	}
	if !strings.Contains(res.Blocked[0].Reason, "nodeSelector") {
		t.Errorf("reason = %q, want nodeSelector mention", res.Blocked[0].Reason)
	}
}

// Each candidate is removable alone, but the set is not: removals must be
// validated jointly, never summed.
func TestJointRemovalValidation(t *testing.T) {
	mk := func(remove []string) SimulateInput {
		return SimulateInput{
			Nodes: []NodeSnapshot{
				node("a", 2000, 4096, f64(100), pod("app", "pa", 1500, 512)),
				node("b", 2000, 4096, f64(100), pod("app", "pb", 1500, 512)),
				node("c", 4000, 8192, f64(100), pod("sys", "rc", 2000, 512)), // 2000m free
			},
			RemoveNodes: remove,
		}
	}
	if res := Simulate(mk([]string{"a"})); res.Status != StatusFeasible {
		t.Fatalf("remove a alone: Status = %s, want %s", res.Status, StatusFeasible)
	}
	if res := Simulate(mk([]string{"b"})); res.Status != StatusFeasible {
		t.Fatalf("remove b alone: Status = %s, want %s", res.Status, StatusFeasible)
	}
	res := Simulate(mk([]string{"a", "b"}))
	if res.Status != StatusInfeasible {
		t.Fatalf("remove a+b: Status = %s, want %s (3000m needed, 2000m free)", res.Status, StatusInfeasible)
	}
}

func TestPodCountCapacity(t *testing.T) {
	target := node("b", 8000, 16384, f64(100), pod("sys", "rb", 100, 128))
	target.Allocatable.Pods = 2 // one resident pod -> exactly one free slot
	res := Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 4000, 8192, f64(100), pod("app", "p1", 100, 128), pod("app", "p2", 100, 128)),
			target,
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusInfeasible {
		t.Fatalf("Status = %s, want %s (only one free pod slot)", res.Status, StatusInfeasible)
	}
}

func TestResourceConservationInvariants(t *testing.T) {
	in := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 4000, 8192, f64(100),
				pod("app", "p1", 1000, 1024), pod("app", "p2", 500, 2048), pod("app", "p3", 250, 512)),
			node("b", 4000, 8192, f64(100), pod("sys", "rb", 2000, 2048)),
			node("c", 4000, 8192, f64(100), pod("sys", "rc", 500, 512)),
		},
		RemoveNodes: []string{"a"},
	}
	res := Simulate(in)
	if res.Status != StatusFeasible {
		t.Fatalf("Status = %s, want %s (blocked: %+v)", res.Status, StatusFeasible, res.Blocked)
	}

	// Each migrated pod has exactly one target, never a removed node.
	seen := map[string]int{}
	for _, p := range res.Placements {
		seen[p.Pod]++
		if p.ToNode == "a" {
			t.Errorf("pod %s placed on removed node a", p.Pod)
		}
		if p.FromNode != "a" {
			t.Errorf("pod %s FromNode = %q, want a", p.Pod, p.FromNode)
		}
	}
	for _, want := range []string{"app/p1", "app/p2", "app/p3"} {
		if seen[want] != 1 {
			t.Errorf("pod %s placed %d times, want exactly 1", want, seen[want])
		}
	}

	// Per-node resource conservation: free = allocatable - used, and
	// used never exceeds allocatable on any dimension.
	plans := map[string]NodePlan{}
	for _, plan := range res.NodePlans {
		plans[plan.Node] = plan
	}
	for _, n := range in.Nodes {
		if n.Name == "a" {
			continue
		}
		plan := plans[n.Name]
		var usedCPU, usedMem, usedPods int64
		for _, p := range n.Pods {
			usedCPU += p.Requests.MilliCPU
			usedMem += p.Requests.MemoryBytes
			usedPods++
		}
		for _, pl := range res.Placements {
			if pl.ToNode != n.Name {
				continue
			}
			for _, p := range in.Nodes[0].Pods {
				if podKey(p.Namespace, p.Name) == pl.Pod {
					usedCPU += p.Requests.MilliCPU
					usedMem += p.Requests.MemoryBytes
					usedPods++
				}
			}
		}
		if usedCPU > n.Allocatable.MilliCPU {
			t.Errorf("node %s CPU over-allocated: %d > %d", n.Name, usedCPU, n.Allocatable.MilliCPU)
		}
		if usedMem > n.Allocatable.MemoryBytes {
			t.Errorf("node %s memory over-allocated: %d > %d", n.Name, usedMem, n.Allocatable.MemoryBytes)
		}
		if plan.Free.MilliCPU != n.Allocatable.MilliCPU-usedCPU {
			t.Errorf("node %s free CPU = %d, want %d", n.Name, plan.Free.MilliCPU, n.Allocatable.MilliCPU-usedCPU)
		}
		if plan.Free.MemoryBytes != n.Allocatable.MemoryBytes-usedMem {
			t.Errorf("node %s free mem = %d, want %d", n.Name, plan.Free.MemoryBytes, n.Allocatable.MemoryBytes-usedMem)
		}
		_ = usedPods
	}
}

func TestInputNotMutated(t *testing.T) {
	in := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 4000, 8192, f64(100), pod("app", "p1", 1000, 1024), pod("app", "p2", 500, 512)),
			node("b", 4000, 8192, f64(100), pod("sys", "rb", 500, 512)),
		},
		RemoveNodes: []string{"a"},
	}
	before, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	Simulate(in)
	after, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("input mutated by Simulate\nbefore: %s\nafter:  %s", before, after)
	}
}

func TestDeterministicUnderShuffledInput(t *testing.T) {
	anti := []PodAffinityTerm{{
		MatchExpressions: []MatchExpression{{Key: "app", Operator: "In", Values: []string{"web"}}},
		TopologyKey:      "kubernetes.io/hostname",
	}}
	p1 := pod("app", "p1", 1000, 1024)
	p1.Labels = map[string]string{"app": "web"}
	p1.RequiredPodAntiAffinity = anti
	p2 := pod("app", "p2", 500, 2048)
	p2.NodeSelector = map[string]string{"pool": "default"}
	p3 := pod("app", "p3", 250, 512)
	p4 := pod("app", "p4", 100, 128)

	mkNodes := func() []NodeSnapshot {
		b := node("b", 4000, 8192, f64(100), pod("sys", "rb", 500, 512))
		b.Labels["pool"] = "default"
		c := node("c", 4000, 8192, f64(100), pod("sys", "rc", 250, 256))
		c.Labels["pool"] = "default"
		return []NodeSnapshot{
			node("a", 4000, 8192, f64(100), p1, p2, p3, p4),
			b, c,
		}
	}
	base := SimulateInput{Nodes: mkNodes(), RemoveNodes: []string{"a"}}
	want := Simulate(base)
	if want.Status != StatusFeasible {
		t.Fatalf("baseline Status = %s, want %s (blocked: %+v, notes: %v)", want.Status, StatusFeasible, want.Blocked, want.Notes)
	}

	// Shuffle: reverse node order, reverse pods within each node,
	// duplicate removal entries.
	shuffled := SimulateInput{RemoveNodes: []string{"a", "a"}}
	nodes := mkNodes()
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	for i := range nodes {
		for x, y := 0, len(nodes[i].Pods)-1; x < y; x, y = x+1, y-1 {
			nodes[i].Pods[x], nodes[i].Pods[y] = nodes[i].Pods[y], nodes[i].Pods[x]
		}
	}
	shuffled.Nodes = nodes
	got := Simulate(shuffled)

	if !reflect.DeepEqual(want, got) {
		wj, _ := json.Marshal(want)
		gj, _ := json.Marshal(got)
		t.Errorf("shuffled input produced a different result\nwant: %s\ngot:  %s", wj, gj)
	}
}

func TestSearchLimitExhaustionReportsUnknown(t *testing.T) {
	var pods []PodSnapshot
	for _, name := range []string{"p1", "p2", "p3", "p4", "p5"} {
		pods = append(pods, pod("app", name, 900, 512))
	}
	in := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 8000, 8192, f64(100), pods...),
			node("b", 4000, 8192, f64(100)),
			node("c", 4000, 8192, f64(100)),
		},
		RemoveNodes:       []string{"a"},
		MaxPlacementSteps: 3, // deliberately tiny
	}
	res := Simulate(in)
	if res.Status != StatusUnknown {
		t.Fatalf("Status = %s, want %s when the search budget is exhausted", res.Status, StatusUnknown)
	}
	if !res.Limits.Exhausted {
		t.Error("Limits.Exhausted = false, want true")
	}
	found := false
	for _, u := range res.Uncertainties {
		if strings.Contains(u, "step budget") {
			found = true
		}
	}
	if !found {
		t.Errorf("Uncertainties = %v, want a step-budget entry", res.Uncertainties)
	}
}

func TestUnknownPriceNeverTreatedAsZero(t *testing.T) {
	res := Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, nil, pod("app", "p1", 100, 128)), // price unknown
			node("b", 2000, 4096, f64(50), pod("app", "p2", 100, 128)),
			node("c", 8000, 16384, f64(200)),
		},
		RemoveNodes: []string{"a", "b"},
	})
	if res.Status != StatusFeasible {
		t.Fatalf("Status = %s, want %s", res.Status, StatusFeasible)
	}
	if res.Savings.NetMonthly != nil {
		t.Errorf("NetMonthly = %v, want nil while a removed node's price is unknown", *res.Savings.NetMonthly)
	}
	if res.Savings.RemovedMonthlyKnown != 50 {
		t.Errorf("RemovedMonthlyKnown = %.2f, want 50 (only the known price)", res.Savings.RemovedMonthlyKnown)
	}
	if len(res.Savings.UnknownPriceNodes) != 1 || res.Savings.UnknownPriceNodes[0] != "a" {
		t.Errorf("UnknownPriceNodes = %v, want [a]", res.Savings.UnknownPriceNodes)
	}
}

func TestPDBProducesRiskNoteOnly(t *testing.T) {
	p := pod("app", "web", 500, 512)
	p.PDB = &PDBInfo{MinAvailable: i32(2)}
	res := Simulate(SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100), p),
			node("b", 4000, 8192, f64(100)),
		},
		RemoveNodes: []string{"a"},
	})
	if res.Status != StatusFeasible {
		t.Fatalf("Status = %s, want %s (PDB is advisory here)", res.Status, StatusFeasible)
	}
	if len(res.RiskNotes) != 1 || !strings.Contains(res.RiskNotes[0], "PodDisruptionBudget") {
		t.Errorf("RiskNotes = %v, want one PDB note", res.RiskNotes)
	}
}

func TestInvalidInputReportedAsUnknown(t *testing.T) {
	res := Simulate(SimulateInput{
		Nodes:       []NodeSnapshot{node("a", 2000, 4096, f64(100))},
		RemoveNodes: []string{"ghost"},
	})
	if res.Status != StatusUnknown {
		t.Errorf("Status = %s, want %s for unknown removal candidate", res.Status, StatusUnknown)
	}

	res = Simulate(SimulateInput{Nodes: []NodeSnapshot{node("a", 2000, 4096, f64(100))}})
	if res.Status != StatusUnknown {
		t.Errorf("Status = %s, want %s for empty removal set", res.Status, StatusUnknown)
	}

	dup := SimulateInput{
		Nodes: []NodeSnapshot{
			node("a", 2000, 4096, f64(100)),
			node("a", 2000, 4096, f64(100)),
		},
		RemoveNodes: []string{"a"},
	}
	if res := Simulate(dup); res.Status != StatusUnknown {
		t.Errorf("Status = %s, want %s for duplicate node names", res.Status, StatusUnknown)
	}
}
