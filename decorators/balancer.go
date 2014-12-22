package metafora

import (
	"regexp"
	"time"

	"github.com/lytics/metafora"
)

/*
	Selectors are associated with tasks

	If any selector matches any label, we return true (For now).   We can look into add
	a more advance lexer/parser for selectors if we want...
*/
type SelectorSet map[string]interface{}

func (s SelectorSet) Matches(l LabelSet) (bool, error) {
	for sel, _ := range s {
		for label, _ := range l {
			matched, err := regexp.MatchString(sel, label)
			if err != nil {
				return false, err
			} else if matched {
				return true, nil
			}
		}
	}
	return false, nil
}

type LabelSet map[string]interface{}

type MetadataClient interface {
	Selectors(taskid string) SelectorSet
	Labels(nodeid string) LabelSet
}

func WrapBalancerAsSelectable(nodeid string, baseBalancer metafora.Balancer, cs metafora.ClusterState, md MetadataClient) metafora.Balancer {
	return &SelectBalancer{
		nodeid: nodeid,
		cs:     cs,
		md:     md,
		bc:     nil,
		base:   baseBalancer,
	}
}

//Add the selectable layer to an existing balancer strategy
type SelectBalancer struct {
	nodeid string
	cs     metafora.ClusterState
	md     MetadataClient
	bc     metafora.BalancerContext
	base   metafora.Balancer // Decorator pattern,
}

func (b *SelectBalancer) Init(s metafora.BalancerContext) {
	b.bc = s
	b.base.Init(s)
}

func (b *SelectBalancer) CanClaim(taskid string) bool {

	selectors := b.md.Selectors(taskid)
	labels := b.md.Labels(b.nodeid)

	if len(selectors) > 0 { //If no selectors defined for task, then just use the base balancer
		matches, err := selectors.Matches(labels)
		if err != nil {
			b.bc.Log(metafora.LogLevelWarn, "Error processing selectors: taskid:%v labels:[%v] selectors:[%v] error:%v", taskid, labels, selectors, err)
			return false
		}
		if !matches {
			return false
		}
	}

	//If all conditions are met, then do fair balancing
	// if this was one of our released tasks then delay claiming it, vs rejecting it
	// We maybe the only node that can reclaim the task due to selectors/labels.
	if !b.base.CanClaim(taskid) {
		time.Sleep(200 * time.Millisecond)
		return true
	}

	return true
}

// Balance releases tasks based on what the base balancer decides
func (b *SelectBalancer) Balance() []string {
	return b.base.Balance()
}
