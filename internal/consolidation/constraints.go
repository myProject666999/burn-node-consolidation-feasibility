package consolidation

import (
	"fmt"
	"strconv"
)

// matchLabels evaluates a list of ANDed match expressions against a label
// set. An empty expression list matches everything (like an empty
// metav1.LabelSelector). An unknown operator returns an error — the
// owning constraint is undecidable and must be surfaced, never ignored.
func matchLabels(exprs []MatchExpression, labels map[string]string) (bool, error) {
	for _, e := range exprs {
		v, present := labels[e.Key]
		switch e.Operator {
		case "In":
			if !present || !contains(e.Values, v) {
				return false, nil
			}
		case "NotIn":
			if present && contains(e.Values, v) {
				return false, nil
			}
		case "Exists":
			if !present {
				return false, nil
			}
		case "DoesNotExist":
			if present {
				return false, nil
			}
		case "Gt", "Lt":
			if !present || len(e.Values) != 1 {
				return false, nil
			}
			have, err1 := strconv.ParseInt(v, 10, 64)
			want, err2 := strconv.ParseInt(e.Values[0], 10, 64)
			if err1 != nil || err2 != nil {
				return false, nil
			}
			if e.Operator == "Gt" && have <= want {
				return false, nil
			}
			if e.Operator == "Lt" && have >= want {
				return false, nil
			}
		default:
			return false, fmt.Errorf("unknown match operator %q on key %q", e.Operator, e.Key)
		}
	}
	return true, nil
}

func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

// toleratesTaint implements Kubernetes toleration semantics: an empty
// operator defaults to Equal; Exists with an empty key matches all keys;
// an empty effect matches all effects.
func toleratesTaint(t Toleration, taint Taint) bool {
	if t.Effect != "" && t.Effect != taint.Effect {
		return false
	}
	op := t.Operator
	if op == "" {
		op = "Equal"
	}
	switch op {
	case "Exists":
		if t.Key == "" {
			return true
		}
		return t.Key == taint.Key
	case "Equal":
		return t.Key == taint.Key && t.Value == taint.Value
	}
	return false
}

// nodeSelectorFailures reports unmet nodeSelector entries.
func nodeSelectorFailures(sel map[string]string, labels map[string]string) []string {
	var out []string
	for k, v := range sel {
		if labels[k] != v {
			out = append(out, fmt.Sprintf("nodeSelector %s=%s", k, v))
		}
	}
	return out
}

// nodeAffinityFailures reports whether the required node affinity (OR of
// terms) is satisfied. Undecidable expressions are returned as errors.
func nodeAffinitySatisfied(terms [][]MatchExpression, labels map[string]string) (bool, error) {
	if len(terms) == 0 {
		return true, nil
	}
	for _, term := range terms {
		ok, err := matchLabels(term, labels)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// untoleratedTaints returns the hard taints (NoSchedule, NoExecute) the
// pod does not tolerate. PreferNoSchedule is a preference and is ignored.
func untoleratedTaints(tolerations []Toleration, taints []Taint) []Taint {
	var out []Taint
	for _, taint := range taints {
		if taint.Effect != "NoSchedule" && taint.Effect != "NoExecute" {
			continue
		}
		tolerated := false
		for _, tol := range tolerations {
			if toleratesTaint(tol, taint) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			out = append(out, taint)
		}
	}
	return out
}

// volumeTopologySatisfied reports whether the node's labels satisfy at
// least one topology term. A nil constraint list is satisfied.
func volumeTopologySatisfied(terms []TopologyTerm, labels map[string]string) bool {
	if len(terms) == 0 {
		return true
	}
	for _, term := range terms {
		ok := true
		for k, values := range term {
			if !contains(values, labels[k]) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// nodeConstraintFailures returns every hard-constraint category the pod
// violates on the node: nodeSelector, required node affinity, untolerated
// taints, and volume topology. Resource capacity and pod anti-affinity
// are checked separately. An error return means a constraint is
// undecidable (unknown operator) and must be surfaced as uncertainty.
func nodeConstraintFailures(p *podState, n *nodeState) ([]string, error) {
	var failures []string
	snap := p.snap

	for _, f := range nodeSelectorFailures(snap.NodeSelector, n.snap.Labels) {
		failures = append(failures, f+" not satisfied")
	}

	affOK, err := nodeAffinitySatisfied(snap.RequiredNodeAffinity, n.snap.Labels)
	if err != nil {
		return nil, err
	}
	if !affOK {
		failures = append(failures, "required node affinity not satisfied")
	}

	for _, t := range untoleratedTaints(snap.Tolerations, n.snap.Taints) {
		failures = append(failures, fmt.Sprintf("untolerated taint %s=%s:%s", t.Key, t.Value, t.Effect))
	}

	if !volumeTopologySatisfied(snap.VolumeTopology, n.snap.Labels) {
		failures = append(failures, "volume topology constraint not satisfied")
	}

	return failures, nil
}

// termMatchesPod reports whether an anti-affinity term owned by ownerNS
// selects the pod (namespace, labels).
func termMatchesPod(term PodAffinityTerm, ownerNS, podNS string, podLabels map[string]string) (bool, error) {
	if len(term.Namespaces) == 0 {
		if podNS != ownerNS {
			return false, nil
		}
	} else if !contains(term.Namespaces, podNS) {
		return false, nil
	}
	return matchLabels(term.MatchExpressions, podLabels)
}

// antiAffinityOK checks required pod anti-affinity in both directions:
// the incoming pod's terms against pods already on nodes in the same
// topology domain, and those pods' terms against the incoming pod. The
// reverse direction is conservative (Kubernetes would not re-evict
// existing pods, but a plan that violates their terms is not one we
// certify). A target node missing the topology key of the incoming pod's
// required term cannot satisfy it and is rejected.
func antiAffinityOK(p *podState, target *nodeState, nodes []*nodeState) (bool, error) {
	// Forward: the incoming pod's own terms.
	for _, term := range p.snap.RequiredPodAntiAffinity {
		domain, ok := target.snap.Labels[term.TopologyKey]
		if !ok {
			// Required term, but the topology key is absent on the
			// target: the term can never be satisfied here.
			return false, nil
		}
		for _, other := range nodes {
			if other.snap.Labels[term.TopologyKey] != domain {
				continue
			}
			for _, q := range other.allPods() {
				match, err := termMatchesPod(term, p.snap.Namespace, q.namespace, q.labels)
				if err != nil {
					return false, err
				}
				if match {
					return false, nil
				}
			}
		}
	}
	// Reverse: terms owned by resident or already-placed pods, evaluated
	// in their own topology domain.
	for _, other := range nodes {
		for _, q := range other.allPods() {
			for _, term := range q.antiAffinity {
				domainQ, okQ := other.snap.Labels[term.TopologyKey]
				domainT, okT := target.snap.Labels[term.TopologyKey]
				if !okQ || !okT || domainQ != domainT {
					continue
				}
				match, err := termMatchesPod(term, q.namespace, p.snap.Namespace, p.snap.Labels)
				if err != nil {
					return false, err
				}
				if match {
					return false, nil
				}
			}
		}
	}
	return true, nil
}
